/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ReplicationVector;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;

/**
 * A dynamic block placement policy that determines where to place the replicas
 * of a block using live performance and capacity statistics. This policy
 * implements a multi-objective optimization problem trying to optimize between
 * load balancing and performance.
 * 
 * @author herodotos.herodotou
 */
public class BlockPlacementPolicyDynamic extends BlockPlacementPolicyDefault {

   private boolean considerLB = true;
   private boolean considerPerf = true;
   private boolean considerUtil = true;

   public BlockPlacementPolicyDynamic() {
      // Nothing to do
   }

   @Override
   public void initialize(Configuration conf, FSClusterStats stats,
         NetworkTopology clusterMap, Host2NodesMap host2datanodeMap) {
      this.considerLB = conf.getBoolean(
            DFSConfigKeys.DFS_DYNAMIC_POLICY_CONSIDER_LOAD_BALANCING_KEY, true);
      this.considerPerf = conf.getBoolean(
            DFSConfigKeys.DFS_DYNAMIC_POLICY_CONSIDER_PERFORMANCE_KEY, true);
      this.considerUtil = conf.getBoolean(
            DFSConfigKeys.DFS_DYNAMIC_POLICY_CONSIDER_UTILIZATION_KEY, true);

      super.initialize(conf, stats, clusterMap, host2datanodeMap);
   }

   /**
    * Choose <i>localMachine</i> as the target. If <i>localMachine</i> is not
    * available, choose a node on the same rack.
    * 
    * May update: excludedNodes, results, rv
    * 
    * @return the chosen storage
    */
   @Override
   protected DatanodeStorageInfo chooseLocalStorage(Node localMachine,
         Set<Node> excludedNodes, long blocksize, int maxNodesPerRack,
         List<DatanodeStorageInfo> results, boolean avoidStaleNodes,
         ReplicationVector rv, boolean fallbackToLocalRack)
         throws NotEnoughReplicasException {
      // if no local machine, randomly choose one node
      if (localMachine == null) {
         return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize,
               maxNodesPerRack, results, avoidStaleNodes, rv);
      }

      DatanodeStorageInfo bestStorage = null;
      if (preferLocalNode && localMachine instanceof DatanodeDescriptor) {
         DatanodeDescriptor localDatanode = (DatanodeDescriptor) localMachine;
         // otherwise try local machine first
         if (excludedNodes.add(localMachine)) { // was not in the excluded list
            // Collect the storage options
            List<StorageOption> options = new ArrayList<BlockPlacementPolicyDynamic.StorageOption>();
            for (DatanodeStorageInfo storage : localDatanode.getStorageInfos()) {
               if (rv.getReplication(storage.getStorageType()) > 0
                     && storage.getRemaining() > blocksize
                           * HdfsConstants.MIN_BLOCKS_FOR_WRITE) {
                  options.add(new StorageOption(storage, 0));
               }
            }

            if (!options.isEmpty()) {
               // Evaluate all options and choose the best one
               bestStorage = chooseBestStorageOption(options, excludedNodes,
                     blocksize, maxNodesPerRack, results, avoidStaleNodes, rv);

               if (bestStorage != null)
                  return bestStorage;
            }
         }
      }

      if (fallbackToLocalRack)
         // try a node on local rack
         return chooseLocalRack(localMachine, excludedNodes, blocksize,
               maxNodesPerRack, results, avoidStaleNodes, rv);
      else
         return null;
   }

   /**
    * Choose <i>numOfReplicas</i> targets from the given <i>scope</i>.
    * 
    * May update: excludedNodes, results, rv
    * 
    * @return the first chosen node, if there is any.
    */
   @Override
   protected DatanodeStorageInfo chooseRandom(int numOfReplicas, String scope,
         Set<Node> excludedNodes, long blocksize, int maxNodesPerRack,
         List<DatanodeStorageInfo> results, boolean avoidStaleNodes,
         ReplicationVector rv) throws NotEnoughReplicasException {

      if (numOfReplicas == 0)
         return null;

      // Compute the set of storage options
      List<StorageOption> options = new ArrayList<BlockPlacementPolicyDynamic.StorageOption>();
      List<Node> nodes;
      if (scope.startsWith("~")) {
         nodes = clusterMap.getLeaves(NodeBase.ROOT);
         nodes.removeAll(clusterMap.getLeaves(scope.substring(1)));
      } else {
         nodes = clusterMap.getLeaves(scope);
      }
      nodes.removeAll(excludedNodes);

      for (Node node : nodes) {
         if (node instanceof DatanodeDescriptor) {
            DatanodeDescriptor dn = (DatanodeDescriptor) node;
            for (DatanodeStorageInfo storage : dn.getStorageInfos()) {
               if (rv.getReplication(storage.getStorageType()) > 0
                     && storage.getRemaining() > blocksize
                           * HdfsConstants.MIN_BLOCKS_FOR_WRITE) {
                  options.add(new StorageOption(storage, 0));
               }
            }
         }
      }

      if (numOfReplicas > options.size()) {
         throw new NotEnoughReplicasException("The number of storage "
               + "options are less than the number of replicas");
      }

      StringBuilder builder = null;
      if (LOG.isDebugEnabled()) {
         builder = debugLoggingBuilder.get();
         builder.setLength(0);
         builder.append("[");
      }

      DatanodeStorageInfo firstChosen = null, bestStorage = null;
      while (numOfReplicas > 0 && !options.isEmpty()) {
         // Evaluate all options and choose the best one
         bestStorage = chooseBestStorageOption(options, excludedNodes,
               blocksize, maxNodesPerRack, results, avoidStaleNodes, rv);

         if (bestStorage == null) {
            // Failed to find any storage options
            break;
         }

         --numOfReplicas;
         if (firstChosen == null)
            firstChosen = bestStorage;

         if (LOG.isDebugEnabled()) {
            builder
                  .append("\nChosenNode ")
                  .append(NodeBase.getPath(bestStorage.getDatanodeDescriptor()))
                  .append("\n]");
         }

         // Update the list of options
         if (numOfReplicas > 0) {
            StorageType chosenType = bestStorage.getStorageType();
            if (rv.getReplication(chosenType) == 0) {
               // Remove all options with this storage type
               Iterator<StorageOption> iter = options.iterator();
               while (iter.hasNext()) {
                  StorageOption option = iter.next();
                  if (option.storage.getStorageType() == chosenType) {
                     iter.remove();
                  }
               }
            }

            // Remove all options from this datanode
            DatanodeDescriptor dn = bestStorage.getDatanodeDescriptor();
            Iterator<StorageOption> iter = options.iterator();
            while (iter.hasNext()) {
               StorageOption option = iter.next();
               if (option.storage.getDatanodeDescriptor().equals(dn)) {
                  iter.remove();
               }
            }
         }
      }

      if (numOfReplicas > 0) {
         String detail = enableDebugLogging;
         if (LOG.isDebugEnabled()) {
            if (builder != null) {
               detail = builder.toString();
               builder.setLength(0);
            } else {
               detail = "Failed to find a storage option";
            }
         }
         throw new NotEnoughReplicasException(detail);
      }

      return firstChosen;
   }

   /**
    * Select the best storage option among the provided ones. First, it computes
    * the load balancing and performance metrics, which are used for computing
    * the final score for each option. The option with the lowest score is
    * selected.
    * 
    * May update: options, excludedNodes, chosenList, rv
    * 
    * @param options
    * @param excludedNodes
    * @param blocksize
    * @param maxNodesPerRack
    * @param chosenList
    * @param avoidStaleNodes
    * @param rv
    * @return
    */
   private DatanodeStorageInfo chooseBestStorageOption(
         List<StorageOption> options, Set<Node> excludedNodes, long blocksize,
         int maxNodesPerRack, List<DatanodeStorageInfo> chosenList,
         boolean avoidStaleNodes, ReplicationVector rv) {

      if (options.isEmpty())
         return null;

      Collections.shuffle(options);
      if (considerLB == false && considerPerf == false && considerUtil == false) {
         // no metric to use - choose randomly
         for (StorageOption option : options) {
            DatanodeStorageInfo storage = option.storage;
            if (addIfIsGoodTarget(storage, excludedNodes, blocksize,
                  maxNodesPerRack, false, chosenList, avoidStaleNodes,
                  storage.getStorageType()) >= 0) {
               rv.decrReplication(storage.getStorageType(), (short) 1);
               return storage;
            }
         }
      }

      // Compute necessary stats from all storage options
      double remainingPerc, maxRemainingPerc = 0d;
      double writeThroughput, maxWriteThroughput = 0d;
      int xceiverCount, minXceiverCount = Integer.MAX_VALUE;
      EnumSet<StorageType> types = EnumSet.noneOf(StorageType.class);

      for (StorageOption option : options) {
         remainingPerc = getPercentage(option.storage.getRemaining(),
               option.storage.getCapacity());
         if (remainingPerc > maxRemainingPerc)
            maxRemainingPerc = remainingPerc;

         writeThroughput = option.storage.getWriteThroughput();
         if (writeThroughput > maxWriteThroughput)
            maxWriteThroughput = writeThroughput;

         xceiverCount = option.storage.getApproXceiverCount();
         if (xceiverCount < minXceiverCount)
            minXceiverCount = xceiverCount;

         types.add(option.storage.getStorageType());
      }
      int minXceiverAdj = minXceiverCount;

      // Compute necessary stats from the already chosen datanodes
      double lMetricChosenSum = 0d;
      double pMetricChosenSum = 0d;
      double uMetricChosenSum = 0d;
      for (DatanodeStorageInfo chosen : chosenList) {
         remainingPerc = getPercentage(chosen.getRemaining(),
               chosen.getCapacity());
         if (remainingPerc > maxRemainingPerc)
            maxRemainingPerc = remainingPerc;

         writeThroughput = chosen.getWriteThroughput();
         if (writeThroughput > maxWriteThroughput)
            maxWriteThroughput = writeThroughput;

         xceiverCount = chosen.getApproXceiverCount();
         if (xceiverCount < minXceiverCount)
            minXceiverCount = xceiverCount;

         lMetricChosenSum += remainingPerc;
         if (writeThroughput > 1)
            pMetricChosenSum += Math.log(writeThroughput);
         uMetricChosenSum += (1.0d / (xceiverCount + 2));
      }

      if (maxWriteThroughput > 1)
         maxWriteThroughput = Math.log(maxWriteThroughput);
      double uMetrixZmax = (1.0d + chosenList.size()) / (minXceiverCount + 2);
      minXceiverAdj = minXceiverAdj - minXceiverCount;
      minXceiverAdj = (minXceiverAdj > 1) ? minXceiverAdj - 1 : 0;

      StorageType preferType = StorageType.DEFAULT;
      StorageType[] aTypes = reverseTiering ? StorageType.valuesCustomOrdered()
            : StorageType.valuesOrdered();
      for (StorageType type : aTypes) {
         if (rv.getReplication(type) > 0) {
            preferType = type;
            break;
         }
      }

      // Evaluate all options and choose the best one
      DatanodeStorageInfo bestStorage = null;
      double bestScore = Double.MAX_VALUE;
      double pWeight = (types.size() == 1) ? 0.01d : 1.0d;
      double uWeight = (types.size() > 1) ? 1.0d / types.size() : 1.0d;

      for (StorageOption option : options) {
         double lMetric = computeLoadBalancingMetric(option.storage, blocksize,
               lMetricChosenSum, maxRemainingPerc, chosenList.size());
         double pMetric = computePerformanceMetric(option.storage,
               pMetricChosenSum, maxWriteThroughput, chosenList.size());
         double uMetric = computeUtilizationMetric(option.storage,
               uMetricChosenSum, uMetrixZmax, minXceiverAdj);

         option.score = Math.sqrt(lMetric * lMetric + pWeight * pMetric
               * pMetric + uWeight * uMetric * uMetric);
         if (option.storage.getStorageType() == preferType) {
            option.score /= 10;
         }

         if (option.score < bestScore) {
            bestScore = option.score;
            bestStorage = option.storage;
         }
      }

      // Check if bestStorage can be used
      if (bestStorage != null
            && addIfIsGoodTarget(bestStorage, excludedNodes, blocksize,
                  maxNodesPerRack, false, chosenList, avoidStaleNodes,
                  bestStorage.getStorageType()) >= 0) {
         rv.decrReplication(bestStorage.getStorageType(), (short) 1);
         return bestStorage;
      }

      // The bestStorage could not be used. Sort and try the rest
      if (options.size() > 1) {
         Collections.sort(options);
         for (int i = 1; i < options.size(); ++i) {
            DatanodeStorageInfo storage = options.get(i).storage;
            if (addIfIsGoodTarget(storage, excludedNodes, blocksize,
                  maxNodesPerRack, false, chosenList, avoidStaleNodes,
                  storage.getStorageType()) >= 0) {
               rv.decrReplication(storage.getStorageType(), (short) 1);
               return storage;
            }
         }
      }

      // No option found
      return null;
   }

   /**
    * Compute the load balancing metric z(x, L) - f(x, L) based on:
    * 
    * f(x, L) = (rem[x] - b) / cap[x] + Sum_l_in_L{ rem[l] / cap[l] }
    * 
    * z(x, L) = maxRemPerc * (|L| + 1)
    * 
    * @param storage
    * @param blockSize
    * @param lMetricChosen
    * @param maxRemainingPerc
    * @param numChosen
    * @return
    */
   private double computeLoadBalancingMetric(DatanodeStorageInfo storage,
         long blockSize, double lMetricChosenSum, double maxRemainingPerc,
         int numChosen) {

      if (!considerLB)
         return 0d;

      double result = maxRemainingPerc * (numChosen + 1);

      result -= lMetricChosenSum;
      result -= getPercentage(storage.getRemaining() - blockSize
            * HdfsConstants.MIN_BLOCKS_FOR_WRITE, storage.getCapacity());

      return result;
   }

   /**
    * Compute the performance metric z(x, L) - f(x, L) based on:
    * 
    * f(x, L) = ( log(perf[x]) + Sum_l_in_L{ log(perf[l]) } ) / log(maxPerf)
    * 
    * z(x, L) = |L| + 1
    * 
    * @param storage
    * @param pMetricChosen
    * @param maxWriteThroughput
    * @param numChosen
    * @return
    */
   private double computePerformanceMetric(DatanodeStorageInfo storage,
         double pMetricChosenSum, double maxWriteThroughput, int numChosen) {

      if (!considerPerf)
         return 0d;

      double result = 0d;

      if (storage.getWriteThroughput() > 1)
         result = Math.log(storage.getWriteThroughput());

      if (maxWriteThroughput > 0)
         result = (result + pMetricChosenSum) / maxWriteThroughput;

      result = numChosen + 1 - result;

      return result;
   }

   /**
    * Compute the performance metric z(x, L) - f(x, L) based on:
    * 
    * f(x) = 1 / (util[x] + 1) + Sum_in_L{ 1 / (util[l] + 1) }
    * 
    * z(x) = (|L| + 1) / (minUtil + 1)
    * 
    * @param storage
    * @param uMetricChosenSum
    * @param uMetrixZmax
    * @return
    */
   private double computeUtilizationMetric(DatanodeStorageInfo storage,
         double uMetricChosenSum, double uMetrixZmax, int minXceiverAdj) {

      if (!considerUtil)
         return 0;

      double result = (1.0d / (storage.getApproXceiverCount() - minXceiverAdj + 2))
            + uMetricChosenSum;

      return uMetrixZmax - result;
   }

   /**
    * Returns the ratio remaining / capacity with appropriate error checks
    * 
    * @param remaining
    * @param capacity
    * @return
    */
   private double getPercentage(long remaining, long capacity) {
      if (remaining > 0 && capacity > 0)
         return ((double) remaining) / capacity;
      else
         return 0d;
   }

   /**
    * A simple class to bundle a storage option with its score
    */
   private static class StorageOption implements Comparable<StorageOption> {

      DatanodeStorageInfo storage;
      double score;

      public StorageOption(DatanodeStorageInfo storage, double score) {
         this.storage = storage;
         this.score = score;
      }

      @Override
      public int compareTo(StorageOption o) {
         if (this.score < o.score)
            return -1;
         else if (this.score > o.score)
            return 1;
         else
            return 0;
      }

      @Override
      public String toString() {
         return "StorageOption [storage=" + storage + ", score=" + score + "]";
      }
   }
}
