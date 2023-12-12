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
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.UnregisteredNodeException;
import org.apache.log4j.Level;

/**
 * @author herodotos.herodotou
 *
 */
public class TestDynamicBlockRetrievalPolicy {

   {
      ((Log4JLogger) BlockPlacementPolicy.LOG).getLogger().setLevel(
            (Level) Level.DEBUG);
      ((Log4JLogger) BlockPlacementPolicyDefault.LOG).getLogger().setLevel(
            (Level) Level.DEBUG);
      ((Log4JLogger) BlockPlacementPolicyDynamic.LOG).getLogger().setLevel(
            (Level) Level.DEBUG);
   }

   private static List<DatanodeDescriptor> datanodes;
   private static DatanodeManager datanodeManager;
   private static Random rand = new Random(1);

   /**
    * @param args
    * @throws IOException
    */
   public static void main(String[] args) throws IOException {
      // Create a cluster
      setupCluster(2, 4, 1, 1, 3);

      DatanodeDescriptor reader = datanodes.get(0);
      List<LocatedBlock> locatedBlocks = new ArrayList<LocatedBlock>(1);

      locatedBlocks.add(new LocatedBlock(new ExtendedBlock("bpid", 0),
            selectStorages(reader, StorageType.SSD, new StorageType[] {
                  StorageType.MEMORY, StorageType.SSD, StorageType.DISK },
                  new StorageType[] { StorageType.MEMORY, StorageType.SSD,
                        StorageType.DISK })));
      datanodeManager.sortLocatedBlocks(reader.getIpAddr(), locatedBlocks);
      print(reader.getHostName(), locatedBlocks.get(0));
      locatedBlocks.clear();

      locatedBlocks.add(new LocatedBlock(new ExtendedBlock("bpid", 0),
            selectStorages(reader, null, new StorageType[] { StorageType.DISK,
                  StorageType.SSD }, new StorageType[] { StorageType.DISK })));
      datanodeManager.sortLocatedBlocks(reader.getIpAddr(), locatedBlocks);
      print(reader.getHostName(), locatedBlocks.get(0));
      
      locatedBlocks.get(0).addCachedLoc(
            DatanodeStorageInfo.toDatanodeInfoWithStorage(Arrays
                  .asList(datanodeManager.getDatanode(
                        locatedBlocks.get(0).getFirstLocation()
                              .getDatanodeUuid()).getStorageInfosType(
                        StorageType.MEMORY))));
      datanodeManager.sortLocatedBlocks(reader.getIpAddr(), locatedBlocks);
      print(reader.getHostName(), locatedBlocks.get(0));
      locatedBlocks.clear();

      locatedBlocks
            .add(new LocatedBlock(new ExtendedBlock("bpid", 0), selectStorages(
                  reader, null, new StorageType[] { StorageType.DISK,
                        StorageType.SSD, StorageType.MEMORY },
                  new StorageType[] {})));
      datanodeManager.sortLocatedBlocks(reader.getIpAddr(), locatedBlocks);
      print(reader.getHostName(), locatedBlocks.get(0));
      locatedBlocks.clear();

      locatedBlocks.add(new LocatedBlock(new ExtendedBlock("bpid", 0),
            selectStorages(reader, null, new StorageType[] {},
                  new StorageType[] { StorageType.DISK, StorageType.DISK,
                        StorageType.SSD, StorageType.MEMORY })));
      datanodeManager.sortLocatedBlocks(reader.getIpAddr(), locatedBlocks);
      print(reader.getHostName(), locatedBlocks.get(0));
      locatedBlocks.clear();

      locatedBlocks.add(new LocatedBlock(new ExtendedBlock("bpid", 0),
            selectStorages(reader, null, new StorageType[] { StorageType.DISK,
                  StorageType.SSD }, new StorageType[] { StorageType.DISK })));
      datanodeManager.sortLocatedBlocks("", locatedBlocks);
      print("N/A", locatedBlocks.get(0));
      locatedBlocks.clear();

      locatedBlocks.add(new LocatedBlock(new ExtendedBlock("bpid", 0),
            selectStorages(reader, null, new StorageType[] { StorageType.DISK,
                  StorageType.DISK }, new StorageType[] { StorageType.DISK })));
      datanodeManager.sortLocatedBlocks("", locatedBlocks);
      print("N/A", locatedBlocks.get(0));
      locatedBlocks.clear();

      locatedBlocks.add(new LocatedBlock(new ExtendedBlock("bpid", 0),
            selectStorages(reader, null, new StorageType[] { StorageType.SSD,
                  StorageType.SSD }, new StorageType[] { StorageType.SSD })));
      datanodeManager.sortLocatedBlocks("", locatedBlocks);
      print("N/A", locatedBlocks.get(0));
      locatedBlocks.clear();
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
   private static void setupCluster(int numRacks, int numNodes, int numMem,
         int numSsd, int numDisks) throws IOException {
      Configuration conf = new HdfsConfiguration();

      conf.setClass(DFSConfigKeys.DFS_BLOCK_RETRIEVAL_POLICY_CLASSNAME_KEY,
            BlockRetrievalPolicyDynamic.class, BlockRetrievalPolicy.class);
      
      conf.setBoolean(
            DFSConfigKeys.DFS_DYNAMIC_POLICY_CONSIDER_UTILIZATION_KEY, true);
      conf.setLong(DFSConfigKeys.DFS_NETWORK_SPEED_RACK_MBPS_KEY,
            DFSConfigKeys.DFS_NETWORK_SPEED_MBPS_DEFAULT);
      conf.getLong(DFSConfigKeys.DFS_NETWORK_SPEED_CLUSTER_MBPS_KEY,
            DFSConfigKeys.DFS_NETWORK_SPEED_MBPS_DEFAULT);

      BlockManagerTestUtil.TestClusterContext context = BlockManagerTestUtil.createCluster(
            conf, numRacks, numNodes, numMem, numSsd, numDisks);
      datanodes = context.datanodes;
      datanodeManager = context.dnManager;
   }

   /**
    * 
    * @param reader
    * @param onLocal
    * @param onRack
    * @param onCluster
    * @return
    */
   private static DatanodeStorageInfo[] selectStorages(
         DatanodeDescriptor reader, StorageType onLocal, StorageType[] onRack,
         StorageType[] onCluster) {
      int count = ((onLocal != null) ? 1 : 0) + onRack.length
            + onCluster.length;
      DatanodeStorageInfo[] storages = new DatanodeStorageInfo[count];
      Set<DatanodeDescriptor> chosen = new HashSet<DatanodeDescriptor>(count);

      int i = 0;

      // Select a local node
      chosen.add(reader);
      if (onLocal != null)
         storages[i++] = reader.getStorageInfosType(onLocal)[0];

      // Select non-rack-local nodes
      for (StorageType type : onCluster) {
         for (DatanodeDescriptor node : datanodes) {
            if (!chosen.contains(node)
                  && !node.getNetworkLocation().equals(
                        reader.getNetworkLocation())) {
               storages[i++] = node.getStorageInfosType(type)[0];
               chosen.add(node);
               break;
            }
         }
      }

      // Select rack-local nodes
      for (StorageType type : onRack) {
         for (DatanodeDescriptor node : datanodes) {
            if (!chosen.contains(node)
                  && node.getNetworkLocation().equals(
                        reader.getNetworkLocation())) {
               storages[i++] = node.getStorageInfosType(type)[0];
               chosen.add(node);
               break;
            }
         }
      }

      for (DatanodeStorageInfo storage : storages) {
         storage.setReaderCountForTesting(rand.nextInt(10));
      }

      return storages;
   }

   /**
    * 
    * @param reader
    * @param lb
    * @throws UnregisteredNodeException
    */
   private static void print(String reader, LocatedBlock lb)
         throws UnregisteredNodeException {
      System.out.print("Reader: " + reader + "\tLocs: ");
      for (DatanodeStorageInfo dsi : datanodeManager.getDatanodeStorageInfos(
            lb.getAllLocations(), lb.getAllStorageIDs())) {
         System.out.print("\t" + dsi.getStorageID());
         System.out.print(" (" + dsi.getReadThroughput() / 1024);
         System.out.print(", " + dsi.getXceiverCount() + ")");
      }
      System.out.println();
   }
}
