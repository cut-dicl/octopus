package org.apache.hadoop.hdfs.server.namenode.replicationmanagement;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ReplicationVector;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStatistics;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;

/**
 * This class includes the Oracle policy to upgrade a file in memory 
 * if it does not have a replica in memory and 
 * it is present in the list of hot files when read this file
 * 
 * @author elena.kakoulli
 */

public class ReplicationUpgradePolicyOracle extends ReplicationUpgradePolicy {

	protected FSDirectory dir;
	protected DatanodeStatistics stats;
	protected Set<String> hotFiles= new HashSet<>();

	@Override
	protected void initialize(Configuration conf, FSDirectory dir, DatanodeStatistics stats) {
		this.dir = dir;
		this.stats = stats;	

		// Read the file with the list of hot files
		File file = new File(conf.get(DFSConfigKeys.DFS_REPLICATION_UPGRADE_POLICY_ORACLE_HOT_FILES_KEY)); 
		Scanner sc = null;
		try {
			sc = new Scanner(file);
		} catch (FileNotFoundException e) {
			LOG.error("Exception in upgrade process: " + e);
		} 

		while (sc.hasNextLine()) 
			hotFiles.add(sc.nextLine());	
	}

	@Override
	protected boolean triggerUpgrade(StorageType type, INodeFile file) {
		if (file == null)
			return false;

		// if the file does not exist in memory and exists in list of hot files return true else false
		long replicationVector = file.getReplicationVector();
		short memCount = ReplicationVector.GetReplication(StorageType.MEMORY, replicationVector);

		return stats.containsStorageTier(StorageType.MEMORY) && memCount == 0 && hotFiles.contains(file.getParent().getFullPathName());
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
		ReplicationVector repV = new ReplicationVector(file.getReplicationVector());
		if (stats.getCapacityRemaining(StorageType.MEMORY) >= file.computeFileSize()) {
			repV.decrReplication(type, (short) 1);
			repV.incrReplication(StorageType.MEMORY, (short) 1);
			return repV.getVectorEncoding();
		}
		return file.getReplicationVector();		
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
