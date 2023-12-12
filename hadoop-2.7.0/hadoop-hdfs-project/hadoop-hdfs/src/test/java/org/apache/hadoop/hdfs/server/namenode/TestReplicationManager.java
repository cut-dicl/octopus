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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ReplicationVector;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockCollection;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyDynamic;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockRetrievalPolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockRetrievalPolicyTiering;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor.BlockTargetPair;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor.BlockTypePair;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.HeartbeatManager;
import org.apache.hadoop.hdfs.server.namenode.replicationmanagement.*;

/**
 * @author herodotos.herodotou
 */
public class TestReplicationManager {

   private static final long BLOCK_SIZE = 128L * 1024 * 1024;
   private static final PermissionStatus PERM = new PermissionStatus(
         "userName", null, FsPermission.getDefault());

   private static Configuration conf;
   private static BlockManager blockManager;
   private static HeartbeatManager heartbeatManager;
   private static FSDirectory fsDirectory;
   private static List<DatanodeDescriptor> datanodes;
   private static ReplicationManager replManager;
   
   private static long nextNodeID = 0;
   private static long nextBlockID = 10000;
   private static long nextBlockGS = 10000;

   private static HashMap<Long, Integer> accessCounts = new HashMap<Long, Integer>();
   
   /**
    * @param args
    * @throws IOException
    */
   public static void main(String[] args) throws IOException {

      // Setup cluster
      setupCluster(1, 9, 1, 1, 3);

      // Create some initial directories and files
      long now = System.currentTimeMillis();
      long mtime = now - 2 * 60 * 60 * 1000;
      INodeDirectory dir1 = createNewDir(fsDirectory.rootDir, "dir_1", mtime);
      for (int i = 1; i <= 6; ++i) {
         createNewFile(dir1, "file_1_" + i, 3L, 2, mtime + i * 10 * 60 * 1000);
      }

      ReplicationVector rv = new ReplicationVector(0);
      rv.incrReplication(StorageType.MEMORY, (short) 2);
      rv.incrReplication(StorageType.SSD, (short) 1);
      rv.incrReplication(StorageType.DISK, (short) 1);
      createNewFile(dir1, "file_1_7", rv.getVectorEncoding(), 2, mtime + 5 * 10 * 60 * 1000);

      BlockManagerTestUtil.printDatanodeStats(datanodes, System.out);
      printFSDirectory(fsDirectory);

      // Setup replication manager
      setupReplicationManager();
      
      // Test file delete
      INodeFile file12 = fsDirectory.getINode("/dir_1/file_1_2").asFile();
      deleteFile(file12);
      replManager.onFileDelete(file12);

      // Test downgrade with new files
      INodeDirectory dir2 = createNewDir(fsDirectory.rootDir, "dir_2",
            System.currentTimeMillis());
      INodeFile newFile = null;
      int fid = 1;
      for (; fid <= 2; ++fid) {
         newFile = createNewFile(dir2, "file_2_" + fid, 3L, 3,
               System.currentTimeMillis());
         replManager.invokeDowngradeProcess(newFile);
         replManager.onFileInfoUpdate(newFile);
         updateReplicationState();
      }

      BlockManagerTestUtil.printDatanodeStats(datanodes, System.out);
      printFSDirectory(fsDirectory);

      // Test (no) upgrade
      INodeFile file13 = fsDirectory.getINode("/dir_1/file_1_3").asFile();
      accessFile(file13);

      // Test downgrade with multiple new files at once
      for (; fid <= 5; ++fid) {
         newFile = createNewFile(dir2, "file_2_" + fid, 3L, 3,
               System.currentTimeMillis());
         replManager.invokeDowngradeProcess(newFile);
         replManager.onFileInfoUpdate(newFile);
      }
      updateReplicationState();

      BlockManagerTestUtil.printDatanodeStats(datanodes, System.out);
      printFSDirectory(fsDirectory);

      // Test upgrade
      for (int i = 0; i < 6; ++i) {
         accessFile(file13);
      }
      updateReplicationState();

      BlockManagerTestUtil.printDatanodeStats(datanodes, System.out);
      printFSDirectory(fsDirectory);
      
      // Test more realistic downgrade and upgrade
      ReplicationDowngradePolicyThreshold.SetDowngradeTriggerThres(85);
      ReplicationDowngradePolicyThreshold.SetDowngradeContinueThres(75);
      ReplicationUpgradePolicyEXD.Threshold = 0.15f;
      INodeDirectory dir3 = createNewDir(fsDirectory.rootDir, "dir_3",
            System.currentTimeMillis());
      INodeFile file3 = null;
      fid = 1;
      int firstfid = 1, lastfid = 0;
      Random r = new Random(1);
      HashMap<Long, Long> rvCounts = new HashMap<Long, Long>();
      long repVector;
      int numDowngrades = 0, numUpgrades = 0;
      
      while (fid <= 100) {
         if (fid <= 10 || r.nextFloat() < 0.2) {
            // Create a new file
            newFile = createNewFile(dir3, "file_3_" + fid, 3L,
                  1 + r.nextInt(15), System.currentTimeMillis());
            numDowngrades += replManager.invokeDowngradeProcess(newFile);
            replManager.onFileInfoUpdate(newFile);
            ++fid;
            
            repVector = newFile.getReplicationVector();
            if (rvCounts.containsKey(repVector))
               rvCounts.put(repVector, new Long(rvCounts.get(repVector) + 1));
            else
               rvCounts.put(repVector, new Long(1));

         } else if (r.nextFloat() < 0.05) {
            // Delete a file
            file3 = fsDirectory.getINode("/dir_3/file_3_" + firstfid).asFile();
            deleteFile(file3);
            replManager.onFileDelete(file3);
            ++firstfid;
         } else {
            // Access an existing file
            file3 = fsDirectory.getINode(
                  "/dir_3/file_3_" + (firstfid + r.nextInt(fid - firstfid)))
                  .asFile();
            numUpgrades += accessFile(file3);
         }

         numDowngrades += updateReplicationState();
         
         if (fid % 5 == 0 && fid != lastfid) {
            System.out.println("FID: " + fid);
            BlockManagerTestUtil.printDatanodeStats(datanodes, System.out);
            for (Map.Entry<Long, Long> entry : rvCounts.entrySet()) {
               System.out.print(ReplicationVector.StringifyReplVector(entry.getKey(),
                     true));
               System.out.println(" \t " + entry.getValue());
            }
            System.out.println("Num downgrades: " + numDowngrades);
            System.out.println("Num upgrades: " + numUpgrades);
            System.out.println("Num deletions: " + (firstfid - 1));
            lastfid = fid;
         }
      }

      numDowngrades += updateReplicationState();
      
      BlockManagerTestUtil.printDatanodeStats(datanodes, System.out);
      printFSDirectory(fsDirectory);
      
      System.out.println("Num downgrades: " + numDowngrades);
      System.out.println("Num upgrades: " + numUpgrades);
      System.out.println("Num deletions: " + (firstfid - 1));
      System.out.println("RV Creation Counts:");
      for (Map.Entry<Long, Long> entry : rvCounts.entrySet()) {
         System.out.print(ReplicationVector.StringifyReplVector(entry.getKey(),
               true));
         System.out.println(" \t " + entry.getValue());
      }
      
      System.out.println();
      System.out.println("Memory Used: "
            + heartbeatManager.getCapacityUsed(StorageType.MEMORY));
      System.out.println("SSD Used:    "
            + heartbeatManager.getCapacityUsed(StorageType.SSD));
      System.out.println("Disk Used:   "
            + heartbeatManager.getCapacityUsed(StorageType.DISK));
   }

   /**
    * 
    * @param numRacks
    * @param numNodes
    * @param numMem
    * @param numSsd
    * @param numDisks
    * @throws IOException
    */
   static void setupCluster(int numRacks, int numNodes, int numMem, int numSsd,
         int numDisks) throws IOException {
      conf = new HdfsConfiguration();

      conf.set(DFSConfigKeys.DFS_DEFAULT_STORAGE_TIER_POLICY_KEY,
            HdfsConstants.DYNAMIC_STORAGE_POLICY_NAME);
      conf.setClass(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY,
            BlockPlacementPolicyDynamic.class, BlockPlacementPolicy.class);
      conf.setClass(DFSConfigKeys.DFS_BLOCK_RETRIEVAL_POLICY_CLASSNAME_KEY,
            BlockRetrievalPolicyTiering.class, BlockRetrievalPolicy.class);

      conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY, 10);
      conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_STREAMS_HARD_LIMIT_KEY, 10);

      BlockManagerTestUtil.TestClusterContext context = BlockManagerTestUtil
            .createCluster(conf, numRacks, numNodes, numMem, numSsd, numDisks);
      blockManager = context.blockManager;
      heartbeatManager = context.heartbeatManager;
      fsDirectory = context.fsDirectory;
      datanodes = context.datanodes;
   }

   /**
    * 
    */
   static void setupReplicationManager() {
      // Downgrade conf
      conf.setClass(
            DFSConfigKeys.DFS_REPLICATION_DOWNGRADE_POLICY_CLASSNAME_KEY,
            ReplicationDowngradePolicyDynamicLRU.class,
            ReplicationDowngradePolicy.class);
      conf.setFloat(
            DFSConfigKeys.DFS_REPLICATION_DOWNGRADE_POLICY_TRIGGER_THRESHOLD_KEY,
            5f);
      conf.setFloat(
            DFSConfigKeys.DFS_REPLICATION_DOWNGRADE_POLICY_CONTINUE_THRESHOLD_KEY,
            1f);
      conf.setInt(
            DFSConfigKeys.DFS_REPLICATION_DOWNGRADE_POLICY_WINDOW_BASED_AGING_KEY,
            1);
      conf.setBoolean(
            DFSConfigKeys.DFS_REPLICATION_DOWNGRADE_ALL_MEM_FILE_KEY,
            false);

      // Upgrade conf
      conf.setClass(DFSConfigKeys.DFS_REPLICATION_UPGRADE_POLICY_CLASSNAME_KEY,
            ReplicationUpgradePolicyMemory.class,
            ReplicationUpgradePolicy.class);
      conf.setInt(
            DFSConfigKeys.DFS_REPLICATION_POLICY_WEIGHTED_BIAS_HOURS_KEY,
            24);
      conf.setInt(
            DFSConfigKeys.DFS_REPLICATION_UPGRADE_POLICY_ACCESS_COUNT_KEY,
            3);
      conf.setInt(
            DFSConfigKeys.DFS_REPLICATION_UPGRADE_POLICY_ACCESS_TIME_KEY,
            1);
      
      replManager = new ReplicationManager(conf, fsDirectory, heartbeatManager,
            blockManager);
      blockManager.setReplicationManager(replManager);
   }
   
   /**
    * Create a new directory
    * 
    * @param parent
    * @param name
    * @return
    * @throws UnsupportedEncodingException
    */
   static INodeDirectory createNewDir(INodeDirectory parent, String name,
         long mtime) throws UnsupportedEncodingException {

      INodeDirectory dir = new INodeDirectory(++nextNodeID,
            name.getBytes("UTF-8"), PERM, mtime);
      parent.addChild(dir);
      fsDirectory.addToInodeMap(dir);

      return dir;
   }

   /**
    * Create a new file
    * 
    * @param parent
    * @param name
    * @param replVector
    * @param numBlocks
    * @return
    * @throws IOException
    */
   static INodeFile createNewFile(INodeDirectory parent, String name,
         long replVector, int numBlocks, long mtime) throws IOException {

      long resolvedRV = blockManager.resolveReplication(replVector, (byte) 0);

      // Create the file
      INodeFile newFile = new INodeFile(++nextNodeID, name.getBytes("UTF-8"),
            PERM, mtime, mtime, BlockInfoContiguous.EMPTY_ARRAY, resolvedRV,
            BLOCK_SIZE);
      parent.addChild(newFile);
      fsDirectory.addToInodeMap(newFile);

      // Create the blocks
      short numReplicas = ReplicationVector.GetTotalReplication(resolvedRV);
      DatanodeStorageInfo[] targets;
      for (int i = 0; i < numBlocks; ++i) {
         BlockInfoContiguous newBlock = new BlockInfoContiguous(new Block(
               ++nextBlockID, BLOCK_SIZE, ++nextBlockGS), numReplicas);
         blockManager.addBlockCollection(newBlock, newFile);
         newFile.addBlock(newBlock);

         targets = blockManager.chooseTarget4NewBlock("", resolvedRV, null,
               null, BLOCK_SIZE, null, HdfsConstants.DYNAMIC_STORAGE_POLICY_ID);
         BlockManagerTestUtil.updateHeartbeats(heartbeatManager, datanodes,
               targets, BLOCK_SIZE, 0, 1, true, true);
         for (DatanodeStorageInfo dsi : targets) {
            dsi.addBlock(newBlock);
         }
      }

      return newFile;
   }

   /**
    * Delete a file
    * 
    * @param file
    */
   static void deleteFile(INodeFile file) {
      for (BlockInfoContiguous block : file.getBlocks()) {
         DatanodeStorageInfo[] dsis = blockManager.getStorages(block);
         for (DatanodeStorageInfo dsi : dsis) {
            // Update stats from removal
            BlockManagerTestUtil.updateHeartbeats(heartbeatManager, datanodes,
                  new DatanodeStorageInfo[] { dsi }, -1 * block.getNumBytes(),
                  0, 0, false, false);
         }

         blockManager.removeBlock(block);
      }

      ArrayList<INodeFile> files = new ArrayList<INodeFile>(1);
      files.add(file);
      fsDirectory.removeFromInodeMap(files);
      file.getParent().removeChild(file);
      file.setParent(null);
      
      updateReplicationState(false, true);
   }
   
   /**
    * Print all files in FSDirectory sorted based on access time
    * 
    * @param fsDirectory
    */
   static void printFSDirectory(FSDirectory fsDirectory) {
      System.out.println("Name\tReplVector\tBlocks\tMTime\tATime\tCount");
      ArrayList<INodeFile> fileList = ReplicationManagementUtils
            .collectSortedFiles(fsDirectory);
      for (INodeFile file : fileList) {
         System.out.println(String.format(
               "%s \t%s\t%d\t%d\t%d\t%d",
               file.getFullPathName(),
               ReplicationVector.StringifyReplVector(
                     file.getReplicationVector(), true),
               file.getBlocks().length, 
               file.getModificationTime(),
               file.getAccessTime(),
               accessCounts.get(file.getId())));
      }
      System.out.println();
   }

   /**
    * Simulate block replication and deletion and update namenode state
    */
   static int updateReplicationState() {
      return updateReplicationState(true, true);
   }
   
   /**
    * Simulate block replication and/or deletion and update namenode state
    */
   static int updateReplicationState(boolean computeRepl, boolean computeDel) {

      int numDowngrades = 0;
      int work = 0;
      if (computeRepl)
         work += BlockManagerTestUtil.computeReplicationWork(blockManager);
      if (computeDel)
         work += BlockManagerTestUtil.computeInvalidationWork(blockManager);
      int count = 0;
      
      while (work > 0 && count < 4) {
         HashSet<BlockCollection> affectedFiles = new HashSet<BlockCollection>();
         List<BlockTargetPair> replCmds = null;
         BlockTypePair[] invCmds = null;

         for (DatanodeDescriptor dn : datanodes) {
            // Process replication commands
            replCmds = dn.getReplicationCommand(Integer.MAX_VALUE);
            if (replCmds != null) {
               for (BlockTargetPair replCmd : replCmds) {
                  BlockInfoContiguous blk = blockManager
                        .getStoredBlock(replCmd.block);
                  affectedFiles.add(blk.getBlockCollection());

                  // Simulate addition of block replicas
                  BlockManagerTestUtil.updateHeartbeats(heartbeatManager, datanodes,
                        replCmd.targets, blk.getNumBytes(), 0, 0, true, false);
                  for (DatanodeStorageInfo dsi : replCmd.targets) {
                     dsi.addBlock(blk);
                     numDowngrades += blockManager.removePendingBlock(blk);
                  }
               }
            }

            // Process deletion commands
            invCmds = dn.getInvalidateBlocks(Integer.MAX_VALUE);
            if (invCmds != null) {
               for (BlockTypePair invCmd : invCmds) {
                  BlockInfoContiguous blk = blockManager
                        .getStoredBlock(invCmd.block);
                  if (blk == null)
                     continue; // block already removed
                  
                  affectedFiles.add(blk.getBlockCollection());

                  DatanodeStorageInfo[] dsis = blockManager.getStorages(blk);
                  for (DatanodeStorageInfo dsi : dsis) {
                     if (dsi.getDatanodeDescriptor().equals(dn)
                           && dsi.getStorageType() == invCmd.type) {
                        // Simulate the removal of a block
                        BlockManagerTestUtil.updateHeartbeats(heartbeatManager,
                              datanodes, new DatanodeStorageInfo[] { dsi }, -1
                                    * blk.getNumBytes(), 0, 0, true, false);

                        dsi.removeBlock(blk);
                        blockManager.removeExcessBlock(dsi, blk);
                        break;
                     }
                  }
               }
            }
         }

         // Check replication of affected files
         for (BlockCollection affectedFile : affectedFiles) {
            blockManager.checkReplication(affectedFile);
         }

         work = 0;
         if (computeRepl)
            work += BlockManagerTestUtil.computeReplicationWork(blockManager);
         if (computeDel)
            work += BlockManagerTestUtil.computeInvalidationWork(blockManager);
         ++count;
      }

      return numDowngrades;
   }
   
   /**
    * Update file access counts and timestamp
    * 
    * @param file
    * @return
    */
   private static int accessFile(INodeFile file) {
      Integer count = accessCounts.get(file.getId());
      if (count == null)
         accessCounts.put(file.getId(), new Integer(1));
      else
         accessCounts.put(file.getId(), new Integer(count + 1));
      
      file.setAccessTime(System.currentTimeMillis());
      replManager.onFileInfoUpdate(file);
      return replManager.invokeUpgradeProcess(file);
   }
}
