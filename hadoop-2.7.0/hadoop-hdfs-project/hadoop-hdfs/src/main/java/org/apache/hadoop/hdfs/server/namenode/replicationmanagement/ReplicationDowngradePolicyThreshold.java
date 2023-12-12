package org.apache.hadoop.hdfs.server.namenode.replicationmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStatistics;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;

import com.google.common.annotations.VisibleForTesting;

/**
 * The downgrade process is triggered based on user-defined threshold values
 * 
 * @author elena.kakoulli
 */
public class ReplicationDowngradePolicyThreshold extends ReplicationDowngradePolicyDefault {

    private static float downgradeTriggerThres;
    private static float downgradeContinueThres;
    
    private static long blockSize;
    
    @Override
    protected void initialize(Configuration conf, FSDirectory directory, DatanodeStatistics stats) {
        super.initialize(conf, directory, stats);
        
        downgradeTriggerThres = conf.getFloat(
              DFSConfigKeys.DFS_REPLICATION_DOWNGRADE_POLICY_TRIGGER_THRESHOLD_KEY,
              DFSConfigKeys.DFS_REPLICATION_DOWNGRADE_POLICY_TRIGGER_THRESHOLD_DEFAULT);
        downgradeContinueThres = conf.getFloat(
              DFSConfigKeys.DFS_REPLICATION_DOWNGRADE_POLICY_CONTINUE_THRESHOLD_KEY,
              DFSConfigKeys.DFS_REPLICATION_DOWNGRADE_POLICY_CONTINUE_THRESHOLD_DEFAULT);
        
        blockSize = HdfsConstants.MIN_BLOCKS_FOR_WRITE
              * conf.getLongBytes(DFSConfigKeys.DFS_BLOCK_SIZE_KEY,
                    DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT);
    }

    @Override
    protected boolean triggerDowngrade(StorageType type, long pendingSize) {
        // trigger when the used percent is greater than the threshold
        return downgrade(type, pendingSize, downgradeTriggerThres, blockSize);
    }

    @Override
    protected boolean continueDowngrade(StorageType type, long pendingSize) {
       // continue while the used percent is above the threshold
       return downgrade(type, pendingSize, downgradeContinueThres, (long) 1.5 * blockSize);
    }
 
    /**
     * Decide whether to downgrade or not. You should downgrade when either holds:
     * (i) the used capacity percent is greater than the 'downgradeThres' threshold;
     * (ii) the remaining capacity per node is lower than the 'remPerNodeThres' threshold
     * 
     * @param type
     * @param pendingSize
     * @param downgradeThres
     * @param remPerNodeThres
     * @return
     */
    private boolean downgrade(StorageType type, long pendingSize,
         float downgradeThres, long remPerNodeThres) {
       // trigger when the used percent is greater than the threshold
       long total = stats.getCapacityTotal(type);
       long used = total - stats.getCapacityRemaining(type) + pendingSize;
       
       if (stats.containsStorageTier(type)) {
          if (used > 0 && total > 0
                && (used * 100d / (double) total) > downgradeThres) {
             return true;
          } else if (stats.getNumNodesInService(type) > 0) {
             long remaining = total - used;
             remaining = remaining / stats.getNumNodesInService(type);
             if (remaining < remPerNodeThres)
                return true;
          }
       }

       return false;
    }

    @VisibleForTesting
    public static void SetDowngradeTriggerThres(float threshold) {
       downgradeTriggerThres = threshold;
    }

    @VisibleForTesting
    public static void SetDowngradeContinueThres(float threshold) {
       downgradeContinueThres = threshold;
    }
}
