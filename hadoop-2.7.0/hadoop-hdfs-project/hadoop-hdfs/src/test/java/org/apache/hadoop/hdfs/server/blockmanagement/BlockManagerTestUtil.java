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

import static org.mockito.Matchers.any;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor.DatanodeDescrTypePairs;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.util.Daemon;
import org.junit.Assert;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.base.Preconditions;

public class BlockManagerTestUtil {
  
  private static Random rand = new Random(1);

  public static void setNodeReplicationLimit(final BlockManager blockManager,
      final int limit) {
    blockManager.maxReplicationStreams = limit;
  }

  /** @return the datanode descriptor for the given the given storageID. */
  public static DatanodeDescriptor getDatanode(final FSNamesystem ns,
      final String storageID) {
    ns.readLock();
    try {
      return ns.getBlockManager().getDatanodeManager().getDatanode(storageID);
    } finally {
      ns.readUnlock();
    }
  }


  /**
   * Refresh block queue counts on the name-node.
   */
  public static void updateState(final BlockManager blockManager) {
    blockManager.updateState();
  }

  /**
   * @return a tuple of the replica state (number racks, number live
   * replicas, and number needed replicas) for the given block.
   */
  public static int[] getReplicaInfo(final FSNamesystem namesystem, final Block b) {
    final BlockManager bm = namesystem.getBlockManager();
    namesystem.readLock();
    try {
      return new int[]{getNumberOfRacks(bm, b),
          bm.countNodes(b).liveReplicas(),
          bm.neededReplications.contains(b) ? 1 : 0};
    } finally {
      namesystem.readUnlock();
    }
  }

  /**
   * @return the number of racks over which a given block is replicated
   * decommissioning/decommissioned nodes are not counted. corrupt replicas 
   * are also ignored
   */
  private static int getNumberOfRacks(final BlockManager blockManager,
      final Block b) {
    final Set<String> rackSet = new HashSet<String>(0);
    final DatanodeDescrTypePairs corruptNodes = 
       getCorruptReplicas(blockManager).getNodes(b);
    for(DatanodeStorageInfo storage : blockManager.blocksMap.getStorages(b)) {
      final DatanodeDescriptor cur = storage.getDatanodeDescriptor();
      if (!cur.isDecommissionInProgress() && !cur.isDecommissioned()) {
        if ((corruptNodes == null ) || !corruptNodes.contains(storage)) {
          String rackName = cur.getNetworkLocation();
          if (!rackSet.contains(rackName)) {
            rackSet.add(rackName);
          }
        }
      }
    }
    return rackSet.size();
  }

  /**
   * @return replication monitor thread instance from block manager.
   */
  public static Daemon getReplicationThread(final BlockManager blockManager)
  {
    return blockManager.replicationThread;
  }
  
  /**
   * Stop the replication monitor thread
   */
  public static void stopReplicationThread(final BlockManager blockManager) 
      throws IOException {
    blockManager.enableRMTerminationForTesting();
    blockManager.replicationThread.interrupt();
    try {
      blockManager.replicationThread.join();
    } catch(InterruptedException ie) {
      throw new IOException(
          "Interrupted while trying to stop ReplicationMonitor");
    }
  }

  /**
   * @return corruptReplicas from block manager
   */
  public static  CorruptReplicasMap getCorruptReplicas(final BlockManager blockManager){
    return blockManager.corruptReplicas;
    
  }

  /**
   * @return computed block replication and block invalidation work that can be
   *         scheduled on data-nodes.
   * @throws IOException
   */
  public static int getComputedDatanodeWork(final BlockManager blockManager) throws IOException
  {
    return blockManager.computeDatanodeWork();
  }
  
  public static int computeInvalidationWork(BlockManager bm) {
    return bm.computeInvalidateWork(Integer.MAX_VALUE);
  }
  
  public static int computeReplicationWork(BlockManager bm) {
     return bm.computeReplicationWork(Integer.MAX_VALUE);
   }

  /**
   * Compute all the replication and invalidation work for the
   * given BlockManager.
   * 
   * This differs from the above functions in that it computes
   * replication work for all DNs rather than a particular subset,
   * regardless of invalidation/replication limit configurations.
   * 
   * NB: you may want to set
   * {@link DFSConfigKeys#DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY} to
   * a high value to ensure that all work is calculated.
   */
  public static int computeAllPendingWork(BlockManager bm) {
    int work = computeInvalidationWork(bm);
    work += bm.computeReplicationWork(Integer.MAX_VALUE);
    return work;
  }

  /**
   * Ensure that the given NameNode marks the specified DataNode as
   * entirely dead/expired.
   * @param nn the NameNode to manipulate
   * @param dnName the name of the DataNode
   */
  public static void noticeDeadDatanode(NameNode nn, String dnName) {
    FSNamesystem namesystem = nn.getNamesystem();
    namesystem.writeLock();
    try {
      DatanodeManager dnm = namesystem.getBlockManager().getDatanodeManager();
      HeartbeatManager hbm = dnm.getHeartbeatManager();
      DatanodeDescriptor[] dnds = hbm.getDatanodes();
      DatanodeDescriptor theDND = null;
      for (DatanodeDescriptor dnd : dnds) {
        if (dnd.getXferAddr().equals(dnName)) {
          theDND = dnd;
        }
      }
      Assert.assertNotNull("Could not find DN with name: " + dnName, theDND);
      
      synchronized (hbm) {
        DFSTestUtil.setDatanodeDead(theDND);
        hbm.heartbeatCheck();
      }
    } finally {
      namesystem.writeUnlock();
    }
  }
  
  /**
   * Change whether the block placement policy will prefer the writer's
   * local Datanode or not.
   * @param prefer if true, prefer local node
   */
  public static void setWritingPrefersLocalNode(
      BlockManager bm, boolean prefer) {
    BlockPlacementPolicy bpp = bm.getBlockPlacementPolicy();
    Preconditions.checkState(bpp instanceof BlockPlacementPolicyDefault,
        "Must use default policy, got %s", bpp.getClass());
    ((BlockPlacementPolicyDefault)bpp).setPreferLocalNode(prefer);
  }
  
  /**
   * Call heartbeat check function of HeartbeatManager
   * @param bm the BlockManager to manipulate
   */
  public static void checkHeartbeat(BlockManager bm) {
    bm.getDatanodeManager().getHeartbeatManager().heartbeatCheck();
  }

  /**
   * Call heartbeat check function of HeartbeatManager and get
   * under replicated blocks count within write lock to make sure
   * computeDatanodeWork doesn't interfere.
   * @param namesystem the FSNamesystem
   * @param bm the BlockManager to manipulate
   * @return the number of under replicated blocks
   */
  public static int checkHeartbeatAndGetUnderReplicatedBlocksCount(
      FSNamesystem namesystem, BlockManager bm) {
    namesystem.writeLock();
    try {
      bm.getDatanodeManager().getHeartbeatManager().heartbeatCheck();
      return bm.getUnderReplicatedNotMissingBlocks();
    } finally {
      namesystem.writeUnlock();
    }
  }

  public static DatanodeStorageInfo updateStorage(DatanodeDescriptor dn,
      DatanodeStorage s) {
    return dn.updateStorage(s);
  }

  /**
   * Call heartbeat check function of HeartbeatManager
   * @param bm the BlockManager to manipulate
   */
  public static void rescanPostponedMisreplicatedBlocks(BlockManager bm) {
    bm.rescanPostponedMisreplicatedBlocks();
  }

  public static DatanodeDescriptor getLocalDatanodeDescriptor(
      boolean initializeStorage) {
    DatanodeDescriptor dn = new DatanodeDescriptor(DFSTestUtil.getLocalDatanodeID());
    if (initializeStorage) {
      dn.updateStorage(new DatanodeStorage(DatanodeStorage.generateUuid()));
    }
    return dn;
  }
  
  public static DatanodeDescriptor getDatanodeDescriptor(String ipAddr,
      String rackLocation, boolean initializeStorage) {
    return getDatanodeDescriptor(ipAddr, rackLocation,
        initializeStorage? new DatanodeStorage(DatanodeStorage.generateUuid()): null);
  }

  public static DatanodeDescriptor getDatanodeDescriptor(String ipAddr,
      String rackLocation, DatanodeStorage storage) {
    return getDatanodeDescriptor(ipAddr, rackLocation, storage, "host");
  }

  public static DatanodeDescriptor getDatanodeDescriptor(String ipAddr,
      String rackLocation, DatanodeStorage storage, String hostname) {
      DatanodeDescriptor dn = DFSTestUtil.getDatanodeDescriptor(ipAddr,
          DFSConfigKeys.DFS_DATANODE_DEFAULT_PORT, rackLocation, hostname);
      if (storage != null) {
        dn.updateStorage(storage);
      }
      return dn;
  }

  public static DatanodeStorageInfo newDatanodeStorageInfo(
      DatanodeDescriptor dn, DatanodeStorage s) {
    return new DatanodeStorageInfo(dn, s);
  }

  public static StorageReport[] getStorageReportsForDatanode(
      DatanodeDescriptor dnd) {
    ArrayList<StorageReport> reports = new ArrayList<StorageReport>();
    for (DatanodeStorageInfo storage : dnd.getStorageInfos()) {
      DatanodeStorage dns = new DatanodeStorage(
          storage.getStorageID(), storage.getState(), storage.getStorageType());
      StorageReport report = new StorageReport(
          dns ,false, storage.getCapacity(),
          storage.getDfsUsed(), storage.getRemaining(),
          storage.getBlockPoolUsed(),
          storage.getWriteThroughput(), storage.getReadThroughput(), 
          storage.getCacheCapacity(), storage.getCacheUsed(), 
          storage.getCacheRemaining(), storage.getCacheBpUsed(),
          storage.getReaderCount(), storage.getWriterCount());
      reports.add(report);
    }
    return reports.toArray(StorageReport.EMPTY_ARRAY);
  }

  /**
   * Have DatanodeManager check decommission state.
   * @param dm the DatanodeManager to manipulate
   */
  public static void recheckDecommissionState(DatanodeManager dm)
      throws ExecutionException, InterruptedException {
    dm.getDecomManager().runMonitor();
  }
  
  public static class TestClusterContext {
     public BlockManager blockManager = null;
     public DatanodeManager dnManager = null;
     public HeartbeatManager heartbeatManager = null;
     public FSDirectory fsDirectory = null;
     public List<DatanodeDescriptor> datanodes = null;
  }
  
  /**
   * Create a new cluster
   * 
   * @param conf
   * @param datanodes
   * @param numRacks
   * @param numNodes
   * @param numMem
   * @param numSsd
   * @param numDisks
   * @throws IOException
   */
  public static TestClusterContext createCluster(Configuration conf,
        int numRacks, int numNodes, int numMem,
        int numSsd, int numDisks) throws IOException {

     conf.setBoolean(
           DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_READ_KEY, false);
     conf.setBoolean(
           DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_WRITE_KEY, false);

     TestClusterContext context = new TestClusterContext();
     
     FSNamesystem fsn = Mockito.mock(FSNamesystem.class);
     Mockito.doReturn(true).when(fsn).hasWriteLock();
     Mockito.doReturn(true).when(fsn).isPopulatingReplQueues();
     context.blockManager = new BlockManager(fsn, conf);
     context.dnManager = context.blockManager.getDatanodeManager();
     context.heartbeatManager = context.dnManager.getHeartbeatManager();
     
     Mockito.doAnswer(new Answer<PermissionStatus>() {
        @Override
        public PermissionStatus answer(InvocationOnMock invocation) throws Throwable {
          Object[] args = invocation.getArguments();
          FsPermission perm = (FsPermission) args[0];
          return new PermissionStatus("superuser", "supergroup", perm);
        }
      }).when(fsn).createFsOwnerPermissions(any(FsPermission.class));
     context.fsDirectory = new FSDirectory(fsn, conf);

     // construct network topology
     context.datanodes = new ArrayList<DatanodeDescriptor>(numRacks * numNodes);
     int dnid = 1;
     for (int r = 1; r <= numRacks; ++r) {
        String rack = "/r" + r;
        for (int n = 1; n <= numNodes; ++n) {
           String node = "n" + dnid;
           DatanodeDescriptor dn = DFSTestUtil.getDatanodeDescriptor(dnid
                 + "." + dnid + "." + dnid + "." + dnid, 2222, rack, node);
           List<StorageReport> reports = new ArrayList<StorageReport>();

           for (int m = 1; m <= numMem; ++m) {
              DatanodeStorage ds = new DatanodeStorage(
                    "m" + ((dnid * 10) + m), DatanodeStorage.State.NORMAL,
                    StorageType.MEMORY);
              dn.updateStorage(ds);
              reports.add(new StorageReport(ds, false,
                    4L * 1024 * 1024 * 1024, 0, 4L * 1024 * 1024 * 1024, 0,
                    118L * 1024, 180L * 1024, 0, 0, 0, 0, 0, 0));
           }

           for (int s = 1; s <= numSsd; ++s) {
              DatanodeStorage ds = new DatanodeStorage(
                    "s" + ((dnid * 10) + s), DatanodeStorage.State.NORMAL,
                    StorageType.SSD);
              dn.updateStorage(ds);
              reports.add(new StorageReport(ds, false,
                    60L * 1024 * 1024 * 1024, 0, 60L * 1024 * 1024 * 1024, 0,
                    32L * 1024, 40L * 1024, 0, 0, 0, 0, 0, 0));
           }

           for (int d = 1; d <= numDisks; ++d) {
              DatanodeStorage ds = new DatanodeStorage(
                    "d" + ((dnid * 10) + d), DatanodeStorage.State.NORMAL,
                    StorageType.DISK);
              dn.updateStorage(ds);
              reports.add(new StorageReport(ds, false,
                    340L * 1024 * 1024 * 1024, 0, 340L * 1024 * 1024 * 1024,
                    0, 13L * 1024, 16L * 1024, 0, 0, 0, 0, 0, 0));
           }

           dn.updateHeartbeat(reports.toArray(new StorageReport[0]), 0, 0,
                 null);
           for (DatanodeStorageInfo dsi : dn.getStorageInfos()) {
              dsi.receivedBlockReport();
           }
           
           context.datanodes.add(dn);
           context.blockManager.getDatanodeManager().addDatanode(dn);
           context.heartbeatManager.addDatanode(dn);
           ++dnid;
        }
     }
     
     return context;
  }

  /**
   * Update the provided storages (targets) with added usage and xceiver counts.
   * Update timestamps for the rest of the datanodes.
   * Randomly decrement some reader/writer counts. 
   * 
   * @param heartbeatManager
   * @param datanodes
   * @param targets
   * @param addedUsage
   * @param addedReaders
   * @param addedWriters
   * @param decrCounts
   * @param incrApprox
   */
   public static void updateHeartbeats(HeartbeatManager heartbeatManager,
         List<DatanodeDescriptor> datanodes,
         DatanodeStorageInfo[] targets, long addedUsage, 
         int addedReaders, int addedWriters, boolean decrCounts, 
         boolean incrApprox) {

      for (DatanodeStorageInfo dsi : targets) {
         DatanodeDescriptor dn = dsi.getDatanodeDescriptor();
         StorageReport[] reports = BlockManagerTestUtil
               .getStorageReportsForDatanode(dn);

         for (int i = 0; i < reports.length; ++i) {
            if (reports[i].getStorage().getStorageID()
                  .equals(dsi.getStorageID())) {
               // Update the storage report with added usage and counts
               long toAdd = Math.min(addedUsage, dsi.getRemaining());
               reports[i] = new StorageReport(new DatanodeStorage(
                     dsi.getStorageID(), dsi.getState(), dsi.getStorageType()),
                     false, dsi.getCapacity(), dsi.getDfsUsed() + toAdd,
                     dsi.getRemaining() - toAdd,
                     dsi.getBlockPoolUsed() + toAdd, dsi.getWriteThroughput(),
                     dsi.getReadThroughput(), dsi.getCacheCapacity(),
                     dsi.getCacheUsed(), dsi.getCacheRemaining(),
                     dsi.getCacheBpUsed(), 
                     dsi.getReaderCount() + addedReaders,
                     dsi.getWriterCount() + addedWriters);
            }
         }

         heartbeatManager.updateHeartbeat(dn, reports, dn.getXceiverCount()
               + addedReaders + addedWriters, 0, null);
      }
      
      if (incrApprox) {
         for (int i = 0; i < addedWriters; ++i)
            DatanodeStorageInfo.incrementBlocksScheduled(targets);
      }
      
      for (DatanodeDescriptor dn : datanodes) {
         StorageReport[] reports = BlockManagerTestUtil
               .getStorageReportsForDatanode(dn);

         for (int i = 0; i < reports.length; ++i) {
            if (decrCounts && rand.nextFloat() < 0.2
                  && (reports[i].getReaderCount() > 0 
                        || reports[i].getWriterCount() > 0
                        || dn.getBlocksScheduled() > 0)) {
               // Decrement xceiver count in report
               StorageReport r = reports[i];
               reports[i] = new StorageReport(r.getStorage(), false,
                     r.getCapacity(), r.getDfsUsed(), r.getRemaining(),
                     r.getBlockPoolUsed(), r.getWriteThroughput(),
                     r.getReadThroughput(), r.getCacheCapacity(),
                     r.getCacheUsed(), r.getCacheRemaining(),
                     r.getCacheBpUsed(), 
                     r.getReaderCount() > 0 ? r.getReaderCount() - 1 : 0,
                     r.getWriterCount() > 0 ? r.getWriterCount() - 1 : 0);
               
               dn.decrementBlocksScheduled(r.getStorage().getStorageType());
            }
         }

         heartbeatManager.updateHeartbeat(dn, reports, dn.getXceiverCount(), 0,
               null);
      }
   }
  

   /**
    * Print remaining percent and xceiver counts of storages
    * 
    * @param datanodes
    * @param out
    */
   public static void printDatanodeStats(List<DatanodeDescriptor> datanodes,
         PrintStream out) {
      out.println("Remaining percent and xceiver counts of storages");
      for (DatanodeDescriptor dn : datanodes) {
         out.print(dn.getNetworkLocation() + "/" + dn.getHostName() + ":");

         DatanodeStorageInfo[] storages = dn.getStorageInfos();
         Arrays.sort(storages, new Comparator<DatanodeStorageInfo>() {
            @Override
            public int compare(DatanodeStorageInfo o1, DatanodeStorageInfo o2) {
               if (o1.getStorageType() == o2.getStorageType())
                  return o1.getStorageID().compareTo(o2.getStorageID());
               else
                  return o1.getStorageType().getOrderValue()
                        - o2.getStorageType().getOrderValue();
            }
         });
         for (DatanodeStorageInfo dsi : storages) {
            out.print(String.format(
                  "\t%.2f\t%d",
                  DFSUtil.getPercentRemaining(dsi.getRemaining(),
                        dsi.getCapacity()), dsi.getXceiverCount()));
         }
         out.println();
      }
      out.println();
   }
}
