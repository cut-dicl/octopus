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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ReplicationVector;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.log4j.Level;

/**
 * @author herodotos.herodotou
 */
public class TestDynamicBlockPlacementPolicy {

   {
      ((Log4JLogger) BlockPlacementPolicy.LOG).getLogger().setLevel(
            (Level) Level.DEBUG);
      ((Log4JLogger) BlockPlacementPolicyDefault.LOG).getLogger().setLevel(
            (Level) Level.DEBUG);
      ((Log4JLogger) BlockPlacementPolicyDynamic.LOG).getLogger().setLevel(
            (Level) Level.DEBUG);
   }

   private static final long BLOCK_SIZE = 128L * 1024 * 1024;
   private static BlockManager blockManager;
   private static HeartbeatManager heartbeatManager;
   private static BlockPlacementPolicy replicator;
   private static List<DatanodeDescriptor> datanodes;

   /**
    * @param args
    * @throws IOException
    */
   public static void main(String[] args) throws IOException {

      Log LOG = LogFactory.getLog(BlockPlacementPolicy.class);
      ((Log4JLogger)LOG).getLogger().setLevel(org.apache.log4j.Level.ERROR);

      // Create a cluster
      setupCluster(1, 11, 1, 1, 3);

      Map<DatanodeDescriptor, Integer> dnCounts = new HashMap<DatanodeDescriptor, Integer>();
      Map<DatanodeStorageInfo, Integer> dsiCounts = new HashMap<DatanodeStorageInfo, Integer>();
      for (DatanodeDescriptor dn : datanodes) {
         dnCounts.put(dn, 0);
         for (DatanodeStorageInfo dsi : dn.getStorageInfos()) {
            dsiCounts.put(dsi, 0);
         }
      }

      // Run experiments
      DatanodeStorageInfo[] targets;
      targets = blockManager.chooseTarget4NewBlock("", 3, datanodes.get(0), null,
            BLOCK_SIZE, null, HdfsConstants.DYNAMIC_STORAGE_POLICY_ID);
      for (int i = 0; i < 1000; ++i) {
         targets = blockManager.chooseTarget4NewBlock("", 3, null, null,
               BLOCK_SIZE, null, HdfsConstants.DYNAMIC_STORAGE_POLICY_ID);

         targets = blockManager.chooseTarget4AdditionalDatanode("", 1, null,
               Arrays.asList(targets), null, BLOCK_SIZE,
               HdfsConstants.DYNAMIC_STORAGE_POLICY_ID);

         List<DatanodeStorageInfo> list = new ArrayList<DatanodeStorageInfo>(
               Arrays.asList(targets));
         chooseExcessReplicates(list, 3);
         targets = list.toArray(new DatanodeStorageInfo[0]);

         for (DatanodeStorageInfo target : targets) {
            dnCounts.put(target.getDatanodeDescriptor(),
                  dnCounts.get(target.getDatanodeDescriptor()) + 1);
            dsiCounts.put(target, dsiCounts.get(target) + 1);
         }

         BlockManagerTestUtil.updateHeartbeats(heartbeatManager, datanodes,
               targets, BLOCK_SIZE, 0, 1, true, false);
         if (i % 10 == 0)
            printStats(i, datanodes, targets, false);
      }

      // Print stats
      System.out.println("Number of selections (storages and totals)");
      for (DatanodeDescriptor dn : datanodes) {
         System.out.print(dn.getNetworkLocation() + "/" + dn.getHostName()
               + ":");

         DatanodeStorageInfo[] storages = dn.getStorageInfos();
         Arrays.sort(storages, new DsiComparator());
         for (DatanodeStorageInfo dsi : storages) {
            System.out.print("\t" + dsiCounts.get(dsi));
         }

         System.out.println("\t" + dnCounts.get(dn));
      }

      System.out.println("Remaining percent and xceiver counts of storages");
      for (DatanodeDescriptor dn : datanodes) {
         System.out.print(dn.getNetworkLocation() + "/" + dn.getHostName()
               + ":");

         DatanodeStorageInfo[] storages = dn.getStorageInfos();
         Arrays.sort(storages, new DsiComparator());
         for (DatanodeStorageInfo dsi : storages) {
            System.out.print(String.format(
                  "\t%.2f\t%d",
                  DFSUtil.getPercentRemaining(dsi.getRemaining(),
                        dsi.getCapacity()), dsi.getXceiverCount()));
         }
         System.out.println();
      }
   }

   /**
    * Create a new cluster
    * 
    * @param numRacks
    * @param numNodes
    * @param numMem
    * @param numSsd
    * @param numDisks
    * @throws IOException
    */
   public static void setupCluster(int numRacks, int numNodes, int numMem,
         int numSsd, int numDisks) throws IOException {
      Configuration conf = new HdfsConfiguration();

      conf.set(DFSConfigKeys.DFS_DEFAULT_STORAGE_TIER_POLICY_KEY,
            HdfsConstants.DYNAMIC_STORAGE_POLICY_NAME);
      conf.setClass(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY,
            BlockPlacementPolicyDynamic.class, BlockPlacementPolicy.class);

      conf.setBoolean(DFSConfigKeys.DFS_DYNAMIC_POLICY_CONSIDER_LOAD_BALANCING_KEY, true);
      conf.setBoolean(DFSConfigKeys.DFS_DYNAMIC_POLICY_CONSIDER_PERFORMANCE_KEY, true);
      conf.setBoolean(DFSConfigKeys.DFS_DYNAMIC_POLICY_CONSIDER_DIVERSITY_KEY, true);
      conf.setBoolean(DFSConfigKeys.DFS_DYNAMIC_POLICY_CONSIDER_UTILIZATION_KEY, true);

      BlockManagerTestUtil.TestClusterContext context = BlockManagerTestUtil.createCluster(
            conf, numRacks, numNodes, numMem, numSsd, numDisks);
      blockManager = context.blockManager;
      datanodes = context.datanodes;
      heartbeatManager = context.heartbeatManager;
      replicator = blockManager.getBlockPlacementPolicy();
   }

   private static void printStats(int iter, List<DatanodeDescriptor> datanodes,
         DatanodeStorageInfo[] targets, boolean details) {

      System.out.print(iter + "\tChosen: ");
      for (DatanodeStorageInfo dsi : targets) {
         System.out.print(dsi.getStorageID() + "\t");
      }

      if (details)
         System.out.println();

      EnumMap<StorageType, Long> remaining = new EnumMap<StorageType, Long>(
            StorageType.class);
      EnumMap<StorageType, Long> capacity = new EnumMap<StorageType, Long>(
            StorageType.class);
      remaining.put(StorageType.MEMORY, 0L);
      capacity.put(StorageType.MEMORY, 0L);
      remaining.put(StorageType.SSD, 0L);
      capacity.put(StorageType.SSD, 0L);
      remaining.put(StorageType.DISK, 0L);
      capacity.put(StorageType.DISK, 0L);

      for (DatanodeDescriptor dn : datanodes) {
         if (details)
            System.out.print(dn.getNetworkLocation() + "/" + dn.getHostName()
                  + ":");

         DatanodeStorageInfo[] storages = dn.getStorageInfos();
         Arrays.sort(storages, new DsiComparator());

         for (DatanodeStorageInfo dsi : storages) {
            remaining.put(dsi.getStorageType(),
                  remaining.get(dsi.getStorageType()) + dsi.getRemaining());
            capacity.put(dsi.getStorageType(),
                  capacity.get(dsi.getStorageType()) + dsi.getCapacity());
            if (details)
               System.out.print(String.format(
                     "\t%.2f\t%d",
                     DFSUtil.getPercentRemaining(dsi.getRemaining(),
                           dsi.getCapacity()), dsi.getXceiverCount()));
         }

         if (details)
            System.out.println();
      }

      System.out.println(String.format("Totals:\t%.2f\t%.2f\t%.2f", DFSUtil
            .getPercentRemaining(remaining.get(StorageType.MEMORY),
                  capacity.get(StorageType.MEMORY)), DFSUtil
            .getPercentRemaining(remaining.get(StorageType.SSD),
                  capacity.get(StorageType.SSD)), DFSUtil.getPercentRemaining(
            remaining.get(StorageType.DISK), capacity.get(StorageType.DISK))));
      if (details)
         System.out.println("====================");
   }

   /**
    * Choose excess replicas to delete
    * 
    * @param nonExcess
    * @param replicationVector
    * @return
    */
   private static List<DatanodeStorageInfo> chooseExcessReplicates(
         final List<DatanodeStorageInfo> nonExcess, long replicationVector) {

      short totalRepl = ReplicationVector
            .GetTotalReplication(replicationVector);
      List<DatanodeStorageInfo> chosen = new ArrayList<DatanodeStorageInfo>(
            totalRepl);
      if (nonExcess.size() <= totalRepl)
         return chosen; // nothing to do

      // first form a rack to datanodes map and
      final BlockStoragePolicy storagePolicy = blockManager
            .getStoragePolicy(HdfsConstants.DYNAMIC_STORAGE_POLICY_ID);
      final List<StorageType> excessTypes = storagePolicy.chooseExcess(
            replicationVector, DatanodeStorageInfo.toStorageTypes(nonExcess));

      final Map<String, List<DatanodeStorageInfo>> rackMap = new HashMap<String, List<DatanodeStorageInfo>>();
      final List<DatanodeStorageInfo> moreThanOne = new ArrayList<DatanodeStorageInfo>();
      final List<DatanodeStorageInfo> exactlyOne = new ArrayList<DatanodeStorageInfo>();

      // split nodes into two sets
      // moreThanOne contains nodes on rack with more than one replica
      // exactlyOne contains the remaining nodes
      replicator
            .splitNodesWithRack(nonExcess, rackMap, moreThanOne, exactlyOne);

      // pick one node to delete that favors the delete hint
      // otherwise pick one with least space from priSet if it is not empty
      // otherwise one node with least space from remains
      while (nonExcess.size() - totalRepl > 0) {
         final DatanodeStorageInfo cur;
         // regular excess replica removal
         cur = replicator.chooseReplicaToDelete(null, null, totalRepl,
               moreThanOne, exactlyOne, excessTypes);

         // adjust rackmap, moreThanOne, and exactlyOne
         replicator.adjustSetsWithChosenReplica(rackMap, moreThanOne,
               exactlyOne, cur);

         nonExcess.remove(cur);
         chosen.add(cur);
      }

      return chosen;
   }

   /**
    * Orders based on storage type and then storage id
    */
   private static class DsiComparator implements
         Comparator<DatanodeStorageInfo> {
      @Override
      public int compare(DatanodeStorageInfo o1, DatanodeStorageInfo o2) {
         if (o1.getStorageType() == o2.getStorageType())
            return o1.getStorageID().compareTo(o2.getStorageID());
         else
            return o1.getStorageType().getOrderValue()
                  - o2.getStorageType().getOrderValue();
      }
   }
}
