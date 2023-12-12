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
import java.util.EnumMap;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ReplicationVector;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;

/**
 * A dynamic block storage policy that determines which storage types to use for
 * creating a file using live performance and capacity statistics. This policy
 * implements a multi-objective optimization problem trying to optimize between
 * load balancing, performance, and storage tier diverity.
 * 
 * @author herodotos.herodotou
 */
public class DynamicBlockStoragePolicy extends BlockStoragePolicy {

   private DatanodeStatistics stats; // Aggregate DFS statistics

   private boolean considerLB = true;
   private boolean considerPerf = true;
   private boolean considerDiv = true;
   private boolean considerUtil = true;
   private Random r = new Random();

   // Parameters for when the policy is used by the replication policies
   private StorageType[] localStorageTypes;
   private long fileSize;
   private StorageType bestType;
   private double bestScore;
   private long blockSize;
   
   private static final long MAX_PENALTY = 256L * 1024 * 1024;
   private static final int LOAD_BAL_BIAS = 260;
   
   /**
    * @param id
    * @param name
    * @param storageTypes
    * @param creationFallbacks
    * @param replicationFallbacks
    * @param copyOnCreateFile
    */
   public DynamicBlockStoragePolicy(byte id, String name,
         StorageType[] storageTypes, StorageType[] creationFallbacks,
         StorageType[] replicationFallbacks, Configuration conf) {
      super(id, name, storageTypes, creationFallbacks, replicationFallbacks,
            false);
      this.stats = null;
      this.considerLB = conf.getBoolean(
            DFSConfigKeys.DFS_DYNAMIC_POLICY_CONSIDER_LOAD_BALANCING_KEY, true);
      this.considerPerf = conf.getBoolean(
            DFSConfigKeys.DFS_DYNAMIC_POLICY_CONSIDER_PERFORMANCE_KEY, true);
      this.considerDiv = conf.getBoolean(
            DFSConfigKeys.DFS_DYNAMIC_POLICY_CONSIDER_DIVERSITY_KEY, true);
      this.considerUtil = conf.getBoolean(
            DFSConfigKeys.DFS_DYNAMIC_POLICY_CONSIDER_UTILIZATION_KEY, true);
      
      this.blockSize = HdfsConstants.MIN_BLOCKS_FOR_WRITE
            * conf.getLongBytes(DFSConfigKeys.DFS_BLOCK_SIZE_KEY,
                  DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT);
      
      localStorageTypes = storageTypes;
      fileSize = 0l;
      bestType = StorageType.DISK;
      bestScore = Double.MAX_VALUE;
   }

   /**
    * Constructor to be used by the replication policies
    * 
    * @param stats
    * @param conf
    */
   public DynamicBlockStoragePolicy(DatanodeStatistics stats, Configuration conf) {
      this(HdfsConstants.DYNAMIC_STORAGE_POLICY_ID,
           HdfsConstants.DYNAMIC_STORAGE_POLICY_NAME,
           new StorageType[]{StorageType.MEMORY, StorageType.SSD, StorageType.DISK},
           new StorageType[]{StorageType.SSD, StorageType.DISK},
           new StorageType[]{StorageType.SSD, StorageType.DISK},
           conf);
      this.stats = stats;
   }
   
   /**
    * @param stats
    *           the stats to set
    */
   public void setStats(DatanodeStatistics stats) {
      this.stats = stats;
   }

   /**
    * @return a list of {@link StorageType}s for storing the replicas of a
    *         block.
    */
   @Override
   public List<StorageType> chooseStorageTypes(final long replicationVector) {
      if (stats == null)
         // Revert back to the static policy
         return super.chooseStorageTypes(replicationVector);

      ReplicationVector resolved = resolveReplicationVector(new ReplicationVector(
            replicationVector));

      // Add a storage type for each replication factor in the vector
      List<StorageType> types = new ArrayList<StorageType>(
            resolved.getTotalReplication());
      short i = 0;
      for (StorageType type : StorageType.valuesOrdered()) {
         short count = resolved.getReplication(type);
         for (i = 0; i < count; ++i)
            types.add(type);
      }

      return types;
   }

   /**
    * Resolve the unspecified replicas in the current replication vector and
    * return a new one without unspecified replicas
    * 
    * @param currRV
    * @return a replication vector without unspecified replicas
    */
   public ReplicationVector resolveReplicationVector(ReplicationVector currRV) {
      bestScore = Double.MAX_VALUE;
      if (stats == null) {
         // Revert back to the static policy
         return super.resolveReplicationVector(currRV);
      }

      short unresolved = currRV.getReplication(null);
      if (unresolved == 0) {
         // Nothing to do
         return new ReplicationVector(currRV);
      }

      if (considerUtil == true && considerDiv == false && considerLB == false
            && considerPerf == false) {
         // Resolved only based on util
         return resolveWeightedRandomly(currRV);
      }

      // Collect the necessary statistics once
      EnumMap<StorageType, Long> remainingMap = new EnumMap<StorageType, Long>(
            StorageType.class);
      EnumMap<StorageType, Long> capacityMap = new EnumMap<StorageType, Long>(
            StorageType.class);
      EnumMap<StorageType, Double> writeThroughputMap = new EnumMap<StorageType, Double>(
            StorageType.class);
      EnumMap<StorageType, Long> remainingPerNodeMap = new EnumMap<StorageType, Long>(
            StorageType.class);
      Long remaining, capacity, minCapacity = Long.MAX_VALUE;
      Float remainingPerc, maxRemainingPerc = 0f;
      Double writeThroughput, minWriteThroughput = Double.MAX_VALUE, maxWriteThroughput = 0d;
      int numMediaPerNode = 1;

      for (StorageType type : StorageType.values()) {
         if (stats.containsStorageTier(type)) {
            remaining = stats.getCapacityRemaining(type);
            remainingMap.put(type, remaining);
            capacity = stats.getCapacityTotal(type);
            capacityMap.put(type, capacity);
            remainingPerc = DFSUtil.getPercentRemaining(remaining, capacity);

            if (stats.getNumNodesInService(type) > 0)
               remaining = remaining / stats.getNumNodesInService(type);
            remainingPerNodeMap.put(type, remaining);
            
            if (capacity > 0 && capacity < minCapacity)
               minCapacity = capacity;
            if (remainingPerc > maxRemainingPerc)
               maxRemainingPerc = remainingPerc;

            numMediaPerNode = 1;
            if (stats.getNumDatanodesInService() > 0)
               numMediaPerNode = stats.getNumNodesInService(type) / stats.getNumDatanodesInService();
            if (numMediaPerNode < 1)
               numMediaPerNode = 1;
            writeThroughput = stats.getAvgWriteThroughput(type) * numMediaPerNode;
            writeThroughputMap.put(type, writeThroughput);

            if (writeThroughput > 1 && writeThroughput < minWriteThroughput)
               minWriteThroughput = writeThroughput;
            if (writeThroughput > maxWriteThroughput)
               maxWriteThroughput = writeThroughput;
         } else {
            remainingMap.put(type, 0L);
            capacityMap.put(type, 0L);
            writeThroughputMap.put(type, 0d);
            remainingPerNodeMap.put(type,  0L);
         }
      }

      if (minCapacity == Long.MAX_VALUE || maxRemainingPerc == 0f
            || maxWriteThroughput == 0d) {
         // Missing stats - revert to static policy
         return super.resolveReplicationVector(currRV);
      }

      maxWriteThroughput = Math.log(minWriteThroughput + maxWriteThroughput);

      // Respect the requested replication factors per type
      final ReplicationVector newRV = new ReplicationVector(0l);
      for (StorageType type : StorageType.values()) {
         short count = currRV.getReplication(type);
         if (count > 0)
            newRV.setReplication(type, count);
      }

      // Distribute the unresolved replicas based on the policy
      StorageType types[] = getStorageTypes();
      for (short i = 0; i < unresolved; ++i) {
         bestType = types[types.length - 1];
         bestScore = Double.MAX_VALUE;
         double currScore = Double.MAX_VALUE;
         float c = 1f;

         for (StorageType currType : types) {
            if (remainingMap.get(currType) <= fileSize ||
                remainingPerNodeMap.get(currType) <= blockSize)
               continue;

            if (currType.isVolatile() && newRV.getTotalReplication() == 0)
               continue;

            // Evaluate the option of adding one replica of this type
            newRV.incrReplication(currType, (short) 1);

            double lMetric = computeLoadBalancingMetric(newRV, remainingMap,
                  capacityMap, minCapacity, maxRemainingPerc);
            double pMetric = computePerformanceMetric(newRV,
                  writeThroughputMap, minWriteThroughput, maxWriteThroughput);
            double dMetric = computeDiversityMetric(newRV);

            currScore = Math.sqrt(lMetric * lMetric + pMetric * pMetric
                  + dMetric * dMetric);

            if (currType.isVolatile() && considerDiv)
               currScore = currScore * newRV.getReplication(currType);

            if (currScore < bestScore
                  || (currScore == bestScore && r.nextFloat() < 1f / ++c)) {
               bestScore = currScore;
               bestType = currType;
            }

            newRV.decrReplication(currType, (short) 1);
         }

         newRV.incrReplication(bestType, (short) 1);
      }

      // Special case for when memory size is small relative to other media
      if (unresolved >= 3 
            && newRV.getReplication(StorageType.MEMORY) == 0
            && stats.containsStorageTier(StorageType.MEMORY)
            && stats.getCapacityRemainingPercent(StorageType.MEMORY) > 10
            && remainingMap.get(StorageType.MEMORY) > fileSize
            && remainingPerNodeMap.get(StorageType.MEMORY) > blockSize) {
         // Replace one other storage type with memory
         for (StorageType type : StorageType.values()) {
            if (newRV.getReplication(type) > 1) {
               newRV.decrReplication(type, (short) 1);
               newRV.incrReplication(StorageType.MEMORY, (short) 1);
            }
         }
      }
      
      return newRV;
   }

   /**
    * Compute the load balancing metric z(rv) - f(rv) based on:
    * 
    * f(rv) = Sum_t{ rv[t] * (100 * (rem[t] - penalty) / cap[t]) }
    * 
    * z(rv) = Sum_t{ rv[t] } * maxRemPerc
    * 
    * @param rv
    * @param remainingMap
    * @param capacityMap
    * @param minCapacity
    * @param maxRemainingPerc
    * @return
    */
   private double computeLoadBalancingMetric(ReplicationVector rv,
         EnumMap<StorageType, Long> remainingMap,
         EnumMap<StorageType, Long> capacityMap, Long minCapacity,
         Float maxRemainingPerc) {

      if (!considerLB)
         return 0d;

      long penalty = fileSize;
      if (penalty == 0)
         penalty = Math.min((long) (0.01 * minCapacity), MAX_PENALTY);
      
      double metric = rv.getTotalReplication() * maxRemainingPerc;

      for (StorageType type : StorageType.values()) {
         if (rv.getReplication(type) > 0)
            metric -= rv.getReplication(type)
                  * DFSUtil.getPercentRemaining(remainingMap.get(type)
                        - penalty, capacityMap.get(type));
      }

      return metric / LOAD_BAL_BIAS;
   }

   /**
    * Compute the performance metric z(rv) - f(rv) based on:
    * 
    * f(rv) = Sum_t{ rv[t] * log(minWt + wt[t]) / log(minWt + maxWt) }
    * 
    * z(rv) = Sum_t{ rv[t] }
    * 
    * @param rv
    * @param writeThroughputMap
    * @param maxWriteThroughput
    * @return
    */
   private double computePerformanceMetric(ReplicationVector rv,
         EnumMap<StorageType, Double> writeThroughputMap,
         Double minWriteThroughput, Double maxWriteThroughput) {

      if (!considerPerf)
         return 0d;

      double metric = 0d;

      for (StorageType type : StorageType.values()) {
         if (rv.getReplication(type) > 0)
            metric += rv.getReplication(type)
                  * Math.log(minWriteThroughput + writeThroughputMap.get(type));
      }

      metric = metric / maxWriteThroughput;
      metric = rv.getTotalReplication() - metric;

      return 2 * metric;
   }

   /**
    * Compute the diversity metric z(rv) - f(rv) based on:
    * 
    * n = number of storage tiers
    * 
    * mean = Sum_t{ rv[t] } / n
    * 
    * f(rv) = sqrt{ Sum_t{ (rv[t] - mean)^2 } / n }
    * 
    * z(rv) = sqrt{ Sum_t{rv[t]}*(1-mean)^2 + (n - Sum_t{rv[t]})*mean^2 / n }
    * 
    * Optimization for when |rv| <= 3:
    * 
    * f(rv) = CountDistinct(rv) / Sum_t{ rv[t] }
    * 
    * z(rv) = 1
    * 
    * @param rv
    * @return
    */
   private double computeDiversityMetric(ReplicationVector rv) {
      if (!considerDiv)
         return 0d;

      if (rv.getTotalReplication() <= 3) {
         return 1.0d - ((double) rv.getDistinctReplFactors() / rv
               .getTotalReplication());
      }

      double mean = ((double) rv.getTotalReplication()) / StorageType.COUNT;

      double variance = 0d;
      for (StorageType type : StorageType.values()) {
         variance += (rv.getReplication(type) - mean)
               * (rv.getReplication(type) - mean);
      }

      double minVar = rv.getTotalReplication() * (1 - mean) * (1 - mean);
      if (rv.getTotalReplication() < StorageType.COUNT)
         minVar += (StorageType.COUNT - rv.getTotalReplication()) * mean * mean;

      return Math.sqrt(variance / StorageType.COUNT)
            - Math.sqrt(minVar / StorageType.COUNT);
   }

   /**
    * Resolve the replication vector by selecting storage types to add randomly
    * but weighted based on the number of nodes for each storage tier.
    * 
    * This method is only used only the utilization optimization metric is
    * enabled.
    * 
    * @param currRV
    * @return
    */
   private ReplicationVector resolveWeightedRandomly(ReplicationVector currRV) {

      EnumMap<StorageType, Integer> nodesMap = new EnumMap<StorageType, Integer>(
            StorageType.class);
      int numNodes, totalNumNodes = 0;

      // Get the node stats
      for (StorageType type : StorageType.values()) {
         if (stats.containsStorageTier(type)) {
            numNodes = stats.getNumNodesInService(type);
            nodesMap.put(type, numNodes);
            totalNumNodes += numNodes;
         } else {
            nodesMap.put(type, 0);
         }
      }

      if (totalNumNodes == 0) {
         // Missing stats - revert to static policy
         return super.resolveReplicationVector(currRV);
      }

      // Give each node a portion of keyspace equal to its relative weight
      // [0, w1) selects d1, [w1, w2) selects d2, etc.
      StorageType types[] = getStorageTypes();
      TreeMap<Integer, StorageType> lottery = new TreeMap<Integer, StorageType>();
      int offset = 0;
      for (StorageType type : types) {
         int weight = Math
               .max(1,
                     (int) ((((float) nodesMap.get(type)) / totalNumNodes) * 1000000));
         if (weight > 0 && stats.getCapacityRemaining(type) > 0) {
            offset += weight;
            lottery.put(offset, type);
         }
      }

      // Respect the requested replication factors per type
      final ReplicationVector newRV = new ReplicationVector(0l);
      for (StorageType type : StorageType.values()) {
         short count = currRV.getReplication(type);
         if (count > 0)
            newRV.setReplication(type, count);
      }

      // Choose numbers from [0, offset), which is the total amount of
      // weight, to select the winner
      short unresolved = currRV.getReplication(null);
      for (short i = 0; i < unresolved; ++i) {
         StorageType winner;
         if (offset == 0) {
            winner = types[types.length - 1];
         } else {
            do {
               winner = lottery.higherEntry(r.nextInt(offset)).getValue();
            } while (winner.isVolatile() && newRV.getTotalReplication() == 0
                  && lottery.size() > 1);
         }

         newRV.incrReplication(winner, (short) 1);
      }

      return newRV;
   }
   
   @Override
   public StorageType[] getStorageTypes() {
      return this.localStorageTypes;
   }

   /**
    * @param storageTypes
    *           storage types to use while resolving a replication vector
    */
   public void setStorageTypes(StorageType[] storageTypes) {
      if (storageTypes.length > 0)
         this.localStorageTypes = storageTypes;
   }

   /**
    * @return the last type that was selected while resolving a replication
    *         vector
    */
   public StorageType getBestType() {
      return bestType;
   }

   /**
    * @return the last score that was computer while resolving a replication
    *         vector
    */
   public double getBestScore() {
      return bestScore;
   }

   /**
    * @param fileSize
    */
   public void setFileSize(long fileSize) {
      this.fileSize = fileSize;
   }
   
   /**
    * @return returns true if at least one storage type was resolved after
    *         invoking the {@link #resolveReplicationVector(ReplicationVector)}
    *         method
    */
   public boolean isResolved() {
      return bestScore != Double.MAX_VALUE;
   }
}
