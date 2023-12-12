package org.apache.hadoop.hdfs.server.namenode.replicationmanagement;

import java.io.IOException;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ReplicationVector;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStatistics;
import org.apache.hadoop.hdfs.server.namenode.FSDirAttrOp;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class includes the replication manager for file 
 * replication downgrade/upgrade process
 * 
 * @author elena.kakoulli
 */

public class ReplicationManager {

    public static final Logger LOG = LoggerFactory.getLogger(ReplicationManager.class);
    
    private final ReplicationDowngradePolicy repDowngradePolicy;
    private final ReplicationUpgradePolicy repUpgradePolicy;
    private final FSDirectory dir;
    private final BlockManager bm;
    private final DatanodeStatistics stats;
    
    // Structures to keep track of pending file replications and their sizes
    private final HashMap<Long, PendingReplication> pendingMap;
    private final EnumMap<StorageType, Size> pendingSizes;
    
    private boolean downgradeAllMem;
    private StringBuilder logMsg = new StringBuilder(128);
    
    private Collection<String> excludedPaths;
    private boolean hasDefaultDownPolicy;
    private long blockSize;
    
    /**
     * Constructor
     * @param conf
     * @param directory
     * @param stats
     * @param blockManager
     */
    public ReplicationManager(Configuration conf, FSDirectory directory, 
          DatanodeStatistics stats, BlockManager blockManager) {
        this.dir = directory;
        this.bm = blockManager;
        this.stats = stats;

        repDowngradePolicy = ReplicationDowngradePolicy.getInstance(conf, dir, stats);
        repUpgradePolicy = ReplicationUpgradePolicy.getInstance(conf, dir, stats);
        
        LOG.info("Downgrade Policy: " + repDowngradePolicy.getClass().getName());
        LOG.info("Upgrade Policy: " + repUpgradePolicy.getClass().getName());
        
        pendingMap = new HashMap<Long, ReplicationManager.PendingReplication>();
        pendingSizes = new EnumMap<StorageType, Size>(StorageType.class);
        for (StorageType type : StorageType.values())
           pendingSizes.put(type, new Size(0l));
        
        downgradeAllMem = conf.getBoolean(
            DFSConfigKeys.DFS_REPLICATION_DOWNGRADE_ALL_MEM_FILE_KEY,
            DFSConfigKeys.DFS_REPLICATION_DOWNGRADE_ALL_MEM_FILE_DEFAULT);
        
        excludedPaths = conf.getStringCollection(
            DFSConfigKeys.DFS_REPLICATION_EXCLUDED_PATHS);
        
        hasDefaultDownPolicy = repDowngradePolicy.getClass().equals(
            DFSConfigKeys.DFS_REPLICATION_DOWNGRADE_POLICY_CLASSNAME_DEFAULT);
        
        blockSize = HdfsConstants.MIN_BLOCKS_FOR_WRITE
              * conf.getLongBytes(DFSConfigKeys.DFS_BLOCK_SIZE_KEY,
                    DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT);
    }

    // invoke after a file read/append/write/setRep
    public void onFileInfoUpdate(INodeFile file) {
      if (file != null && !isExcluded(file)) {
         repDowngradePolicy.onFileInfoUpdate(file);
         repUpgradePolicy.onFileInfoUpdate(file);
      }
    }

    // invoke after a file delete
    public void onFileDelete(INodeFile file) {
      if (file != null && !isExcluded(file)) {
         repDowngradePolicy.onFileDelete(file);
         repUpgradePolicy.onFileDelete(file);
         
         deletePendingReplication(file.getId());
      }
    }
    
    /**
    * Invoke replication downgrade process after a file write
    * 
    * @param file
    *           the file that was just written
    * @return number of files downgraded
    */
    public int invokeDowngradeProcess(INodeFile file) {
       long repl = file.getReplicationVector();
       if (downgradeAllMem && bm.getDefaultStoragePolicy()
             .getId() == HdfsConstants.ALLMEM_STORAGE_POLICY_ID) {
          // If all replicas are in memory, downgrade one
          ReplicationVector rv = new ReplicationVector(repl);
          if (rv.getReplication(StorageType.MEMORY) == rv.getTotalReplication()) {
             downgradeFile(file, StorageType.MEMORY);
          }
       }
       
       return invokeDowngradeProcess(repl);
    }

    /**
     * Internal method to invoke downgrade process based on the downgrade policy
     * 
     * @param replicationVector
     * @return
     */
    private int invokeDowngradeProcess(long replicationVector) { 
        StorageType lowestTier = ReplicationManagementUtils.getLowestTier(stats);
        StorageType[] types = StorageType.valuesOrdered();
        int numDowngrades = 0;
        short repVector;
        for (int i = 0; i < types.length; i++) {
            repVector = ReplicationVector.GetReplication(types[i], replicationVector);
            if (repVector > 0 || types[i] == StorageType.MEMORY) {
               if (!types[i].equals(lowestTier)
                     && repDowngradePolicy.triggerDowngrade(types[i],
                           pendingSizes.get(types[i]).getSize())) {
                    boolean success;
                    INodeFile fileToDowngrade;
                    do {
                        success = false;
                        fileToDowngrade = repDowngradePolicy.selectFileToDowngrade(types[i]);
                        if (fileToDowngrade != null) {
                           success = downgradeFile(fileToDowngrade, types[i]);
                           if (success) {
                              ++numDowngrades;
                           }
                        }
                    } while (success && numDowngrades < 20 
                          && repDowngradePolicy.continueDowngrade(types[i], 
                                pendingSizes.get(types[i]).getSize()));
                }
            }
        }
        
        return numDowngrades;
    }

   /**
    * Downgrade a file from the given storage type.
    * 
    * @param file
    * @param type
    * @return
    */
   private boolean downgradeFile(INodeFile file, StorageType type) {
      if (isExcluded(file))
         return false;
      
      // Use the policy to select target tier
      long oldReplVector = file.getReplicationVector();
      long newReplVector = repDowngradePolicy.downgradeFile(file, type);
      if (newReplVector == oldReplVector)
         return false;

      StorageType to = getDiffStorageType(oldReplVector, newReplVector, type);
      if (to == null) {
         LOG.error("Cannot determine target downgrade tier. Old RV= "
               + ReplicationVector.StringifyReplVector(oldReplVector)
               + " New RV= "
               + ReplicationVector.StringifyReplVector(newReplVector));
      }

      boolean success = false;
      try {
         success = FSDirAttrOp.setReplication(dir, bm, file, newReplVector);
      } catch (IOException e) {
         LOG.error("Exception in downgrade process: " + e);
      }

      if (success) {
         long fileSize = file.computeFileSize();
         recordReplication(file, oldReplVector, newReplVector, type, to, fileSize);
         logMsg.setLength(0); // clear builder
         logMsg.append("Done a Downgrade Process from ");
         logMsg.append(type);
         logMsg.append(" to ");
         logMsg.append(to);
         logMsg.append(" for file ");
         logMsg.append(file.getName());
         logMsg.append(" with size ");
         logMsg.append(fileSize);
         LOG.info(logMsg.toString());
      }

      return success;
   }
    
   /**
    * Invoke replication upgrade process after a file read
    * 
    * @param file
    *           the file that was just accessed
    */
    public int invokeUpgradeProcess(INodeFile file) { 

        long replicationVector = file.getReplicationVector();
        StorageType highestTier = ReplicationManagementUtils.getHighestTier(stats);
        StorageType[] types = StorageType.valuesOrdered();
        int numUpgrades = 0;
        
        for (int i = 0; i < types.length; i++) {
            short repVector = ReplicationVector.GetReplication(types[i], replicationVector);
            if (repVector > 0) {
                if (!types[i].equals(highestTier) 
                      && repUpgradePolicy.triggerUpgrade(types[i], file)) {
                   
                    boolean success;
                    INodeFile fileToUpgrade;
                    do {
                        success = false;
                        fileToUpgrade = repUpgradePolicy.selectFileToUpgrade(file, types[i]);
                        if (fileToUpgrade != null) {
                            success = upgradeFile(fileToUpgrade, types[i]);
                            if (success) {
                                ++numUpgrades;
                            }
                            file = null; //to continue with other files
                        }
                    } while (success && numUpgrades < 20 
                          && repUpgradePolicy.continueUpgrade(types[i]));
                }
            }
        }
        
        if (numUpgrades > 0) {
           // Check for possible downgrades
           ReplicationVector rv = new ReplicationVector(0);
           for (StorageType type : StorageType.values())
              if (stats.containsStorageTier(type))
                 rv.incrReplication(type, (short) 1);
           
           invokeDowngradeProcess(rv.getVectorEncoding());
        }
        
        return numUpgrades;
    }

    /**
    * Upgrade a file from the given storage type.
    * 
    * @param file
    * @param type
    * @return
    */
   private boolean upgradeFile(INodeFile file, StorageType type) {
      if (isExcluded(file))
         return false;
      
      // Use the policy to select target tier
      long oldReplVector = file.getReplicationVector();
      long newReplVector = repUpgradePolicy.upgradeFile(file, type);
      if (newReplVector == oldReplVector)
         return false;

      StorageType to = getDiffStorageType(oldReplVector, newReplVector, type);
      if (to == null) {
         LOG.error("Cannot determine target upgrade tier. Old RV= "
               + ReplicationVector.StringifyReplVector(oldReplVector)
               + " New RV= "
               + ReplicationVector.StringifyReplVector(newReplVector));
         return false;
      }
      
      long fileSize = file.computeFileSize();
      if (hasDefaultDownPolicy) {
         // No downgrade policy. Check storage tier capacity
         long remaining = stats.getCapacityRemaining(to) - pendingSizes.get(to).getSize(); 
         if (remaining < fileSize)
            return false;
         else if (stats.getNumNodesInService(type) > 0) {
            remaining /= stats.getNumNodesInService(type);
            if (remaining < blockSize)
               return false;
         }
      } else if (repDowngradePolicy.triggerDowngrade(to, 0)) {
         if (LOG.isDebugEnabled()) {
            LOG.debug("Cancel upgrading file " + file.getName() + " to " + to);
         }
         return false;
      }

      boolean success = false;
      try {
         success = FSDirAttrOp.setReplication(dir, bm, file, newReplVector);
      } catch (IOException e) {
         LOG.error("Exception in upgrade process: " + e);
      }

      if (success) {
         recordReplication(file, oldReplVector, newReplVector, type, to, fileSize);
         logMsg.setLength(0); // clear builder
         logMsg.append("Done a Upgrade Process from ");
         logMsg.append(type);
         logMsg.append(" to ");
         logMsg.append(to);
         logMsg.append(" for file ");
         logMsg.append(file.getName());
         logMsg.append(" with size ");
         logMsg.append(fileSize);
         LOG.info(logMsg.toString());
      }

      return success;
   }
   
    /**
    * To be called each time after a file block was just replicated
    * 
    * @param file
    *           the file that is been replicated
    * @param block
    *           the block that was just replicated
    *           
    * @return number of files downgraded
    */
    public int invokePostReplicationProcess(INodeFile file, Block block) {
       
       if (pendingMap.containsKey(file.getId())) {
          // A file block was just replicated
          PendingReplication pending = pendingMap.get(file.getId());
          long blockSize = block.getNumBytes();
          
          updatePendingSizes(pending, blockSize);
          
          if (!pending.hasMorePending()) {
             // The entire file was just replicated
             return invokePostReplicationProcess(file);
          }
       } else {
          // A file is replicated directly from the user
          return invokeDowngradeProcess(file.getReplicationVector());
       }
       
       return 0;
    }
   
    /**
    * Invoke replication downgrade process after an entire file was replicated
    * 
    * @param file
    *           the file that was just replicated
    * @return number of files downgraded
    */
    private int invokePostReplicationProcess(INodeFile file) {
       long diffReplVector;
       PendingReplication pending = pendingMap.remove(file.getId());
       
       if (LOG.isDebugEnabled()) {
          LOG.debug("The file has been replicated: " + file.getName()
           + " pending=" + ((pending==null) ? "false" : "true"));
       }
       
       if (pending != null) {
          diffReplVector = pending.getForwardDiff().getVectorEncoding();
          if (downgradeAllMem && bm.getDefaultStoragePolicy()
                .getId() == HdfsConstants.ALLMEM_STORAGE_POLICY_ID) {
             // If more than 1 replica in memory, downgrade one
             if (file.getReplicationFactor(StorageType.MEMORY) > 1)
                downgradeFile(file, StorageType.MEMORY);
          }
       }
       else {
          // The user has modified the replication vector of the file
          diffReplVector = file.getReplicationVector();
       }
       
       return invokeDowngradeProcess(diffReplVector);
    }
    
    /**
     * Record pending file replication
     * 
     * @param id
     * @param oldReplVector
     * @param newReplVector
     * @param from
     * @param to
     * @param size
     */
    private void recordReplication(INodeFile file, long oldReplVector,
         long newReplVector, StorageType from, StorageType to, long size) {

       PendingReplication pending = pendingMap.get(file.getId());
       if (pending == null) {
         pendingMap.put(file.getId(), new PendingReplication(oldReplVector,
               newReplVector, from, to, size));
       } else {
          pending.update(oldReplVector, newReplVector, from, to, size);
          if (!pending.hasMorePending() 
                || pending.getOldRV() == pending.getNewRV()) {
             // A series of up/downgrades are canceling themselves out
             deletePendingReplication(file.getId());
          }
       }
       
       pendingSizes.get(from).decrease(size); // expected decrease from "from" tier
       pendingSizes.get(to).increase(size); // expected increase to the "to" tier
       
       // Check if the replication will actually take place
       BlockInfoContiguous[] blocks = file.getBlocks();
       if (blocks != null && blocks.length > 0) {
          if (!bm.containsNeededReplications(blocks[0])) {
             deletePendingReplication(file.getId());
          }
       }
    }

    /**
    * Update pending sizes after a pending replication completes or the file is
    * deleted.
    * 
    * @param pending
    * @param size
    */
    private void updatePendingSizes(PendingReplication pending, long size) {
       ReplicationVector forwardRV = pending.getForwardDiff();
       ReplicationVector backwardRV = pending.getBackwardDiff();

       for (StorageType type : StorageType.values()) {
          // Decrement the "to" type
          if (forwardRV.getReplication(type) > 0) {
             pendingSizes.get(type).decrease(size);
             pending.getSize(type).decrease(size);
          }

          // Increment the "from" type
          if (backwardRV.getReplication(type) > 0) {
             pendingSizes.get(type).increase(size);
             pending.getSize(type).increase(size);
          }
       }
    }

    /**
     * Delete a pending replication from the pending map and update the
     * pending sizes
     * 
     * @param id
     */
    private void deletePendingReplication(long id) {
       PendingReplication pending = pendingMap.remove(id);
       if (pending != null) {
          for (StorageType type : StorageType.values()) {
             pendingSizes.get(type).decrease(
                   pending.getSize(type).getSize());
          }
       }
    }
    
    /**
     * Checks if the file name contains an excluded path
     * @param file
     * @return
     */
    private boolean isExcluded(INodeFile file) {
       if (excludedPaths.isEmpty())
          return false;
       
       String name = file.getName();
       for (String excludedPath : excludedPaths) {
          if (name.contains(excludedPath))
             return true;
       }
       
       return false;
    }
    
    private static StorageType getDiffStorageType(long oldReplVector,
         long newReplVector, StorageType type) {
      ReplicationVector diffRV = new ReplicationVector(newReplVector);
      diffRV.incrReplication(type, (short) 1);
      diffRV.subtractReplicationVector(new ReplicationVector(oldReplVector));
      for (StorageType t : StorageType.values())
         if (diffRV.getReplication(t) > 0)
            return t;
      
      return null;
    }
    
    /**
     * A simple class to store the old and new replication vector of a file
     * after a downgrade or upgrade
     */
    private class PendingReplication {
       private ReplicationVector oldReplVector;
       private ReplicationVector newReplVector;
       private EnumMap<StorageType, Size> pendingSizes;
       
       public PendingReplication(long oldReplVector, long newReplVector,
             StorageType from, StorageType to, long size) {
          this.oldReplVector = new ReplicationVector(oldReplVector);
          this.newReplVector = new ReplicationVector(newReplVector);
          
          pendingSizes = new EnumMap<StorageType, Size>(StorageType.class);
          for (StorageType type : StorageType.values())
             pendingSizes.put(type, new Size(0l));
          pendingSizes.get(from).decrease(size); // expected decrease from "from" tier
          pendingSizes.get(to).increase(size); // expected increase to the "to" tier
      }

      public long getNewRV() {
         return newReplVector.getVectorEncoding();
      }

      public long getOldRV() {
         return oldReplVector.getVectorEncoding();
      }

      public ReplicationVector getForwardDiff() {
         // return new minus old replication
         ReplicationVector rvDiff = new ReplicationVector(newReplVector);
         rvDiff.subtractReplicationVector(oldReplVector);
         return rvDiff;
      }

      public ReplicationVector getBackwardDiff() {
         // return old minus new replication
         ReplicationVector rvDiff = new ReplicationVector(oldReplVector);
         rvDiff.subtractReplicationVector(newReplVector);
         return rvDiff;
      }

      public void update(long oldReplVector, long newReplVector, 
            StorageType from, StorageType to, long size) {
         if (this.newReplVector.getVectorEncoding() == oldReplVector)
            this.newReplVector.setVectorEncoding(newReplVector); // cascade change
         else {
            this.oldReplVector.setVectorEncoding(oldReplVector);
            this.newReplVector.setVectorEncoding(newReplVector);
         }

         pendingSizes.get(from).decrease(size); // expected decrease from "from" tier
         pendingSizes.get(to).increase(size); // expected increase to the "to" tier
      }

      public Size getSize(StorageType type) {
         return pendingSizes.get(type);
      }
      
      public boolean hasMorePending() {
         for (StorageType type : StorageType.values()) {
            if (pendingSizes.get(type).getSize() > 0)
               return true;
         }
         
         return false;
      }

      @Override
      public String toString() {
         return "Old: " + oldReplVector + " New: " + newReplVector 
               + " sizes: " + pendingSizes;
      }
    }
    
    private class Size {
       long size;
       
       public Size(long size) {
          this.size = size;
      }

      void increase(long s) {
          size += s;
       }
       
       void decrease(long s) {
          size -= s;
       }
       
       long getSize() {
          return size;
       }
       
       @Override
       public String toString() {
          return Long.toString(size);
       }
    }
}
