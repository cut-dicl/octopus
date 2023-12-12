package org.apache.hadoop.hdfs.server.namenode.replicationmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStatistics;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;

/**
 * This class includes the default policy to select a file
 * for downgrade. By default, it does nothing.
 * 
 * 
 * @author elena.kakoulli
 */
public class ReplicationDowngradePolicyDefault extends ReplicationDowngradePolicy {

    protected FSDirectory dir;
    protected DatanodeStatistics stats;
    
    @Override
   protected void initialize(Configuration conf, FSDirectory directory,
         DatanodeStatistics statistics) {
        this.dir = directory;
        this.stats = statistics;
    }

    @Override
    protected boolean triggerDowngrade(StorageType type, long pendingSize) {
        return false;
    }

    @Override
    protected boolean continueDowngrade(StorageType type, long pendingSize) {
        return false;
    }
    
    @Override
    protected INodeFile selectFileToDowngrade(StorageType type) {
        return null;
    }

    @Override
    protected long downgradeFile(INodeFile file, StorageType type) {
        return ReplicationManagementUtils.selectOneTierDown(file, type, stats);
    }

    @Override
    protected void onFileInfoUpdate(INodeFile file) {
       // Nothing to do	
    }
	
    @Override
    protected void onFileDelete(INodeFile file){
       // Nothing to do
    }
}
