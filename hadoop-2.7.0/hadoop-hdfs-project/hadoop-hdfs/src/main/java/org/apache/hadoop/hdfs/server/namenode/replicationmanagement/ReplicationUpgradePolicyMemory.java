package org.apache.hadoop.hdfs.server.namenode.replicationmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ReplicationVector;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStatistics;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;

/**
 * This class includes the memory policy to upgrade a file in memory 
 * if it does not exist in memory when read this file
 * 
 * 
 * @author elena.kakoulli
 */

public class ReplicationUpgradePolicyMemory extends ReplicationUpgradePolicy {

	protected FSDirectory dir;
	protected DatanodeStatistics stats;

	@Override
	protected void initialize(Configuration conf, FSDirectory dir, DatanodeStatistics stats) {
		this.dir = dir;
		this.stats = stats;		
	}

	@Override
	protected boolean triggerUpgrade(StorageType type, INodeFile file) {
	   if (file == null)
	      return false;
	   
		// if the file does not exist in memory return true else false
		long replicationVector = file.getReplicationVector();
		short memCount = ReplicationVector.GetReplication(StorageType.MEMORY, replicationVector);

		return stats.containsStorageTier(StorageType.MEMORY) && memCount == 0;
	}

	@Override
	protected boolean continueUpgrade(StorageType type) {
		return false;
	}

	@Override
	protected INodeFile selectFileToUpgrade(INodeFile file, StorageType type) {
		if (!file.isUnderConstruction())
			return file;
		else
			return null;
	}

	@Override
	protected long upgradeFile(INodeFile file, StorageType type) {
		return ReplicationManagementUtils.selectMemoryTier(file, type, stats);
	}

	@Override
	protected void onFileInfoUpdate(INodeFile file) {
		// nothing to do		
	}

	@Override
	protected void onFileDelete(INodeFile file) {
		// nothing to do		
	}

}
