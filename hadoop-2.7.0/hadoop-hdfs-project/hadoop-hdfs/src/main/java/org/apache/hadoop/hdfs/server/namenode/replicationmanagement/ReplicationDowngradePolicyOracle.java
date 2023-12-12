package org.apache.hadoop.hdfs.server.namenode.replicationmanagement;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
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
 * This class includes the Oracle policy to downgrade a file 
 * if it has a replica in memory and it is present in the list of cold files
 * 
 * @author elena.kakoulli
 */

public class ReplicationDowngradePolicyOracle extends
ReplicationDowngradePolicyThreshold {

	protected AccessBasedList dirLRU;
	protected Set<String> coldFiles= new HashSet<>();

	@Override
	protected void initialize(Configuration conf, FSDirectory dir,
			DatanodeStatistics statistics) {
		super.initialize(conf, dir, statistics);

		// Creation of LRU list with only files of memory tier
		this.dirLRU = new AccessBasedList();
		ArrayList<INodeFile> fileList = ReplicationManagementUtils.collectSortedFiles(dir); 
		for (INodeFile file : fileList)
			if (ReplicationVector.GetReplication(StorageType.MEMORY,file.getReplicationVector()) > 0)
				dirLRU.addFile(file); 

		// Read the file with the list of cold files
		File file = new File(conf.get(DFSConfigKeys.DFS_REPLICATION_DOWNGRADE_POLICY_ORACLE_COLD_FILES_KEY)); 
		Scanner sc = null;
		try {
			sc = new Scanner(file);
		} catch (FileNotFoundException e) {
			LOG.error("Exception in downgrade process: " + e);
		} 

		while (sc.hasNextLine()) 
			coldFiles.add(sc.nextLine());
	}

	@Override
	protected INodeFile selectFileToDowngrade(StorageType type) {
		Iterator<INodeFile> LRUfiles = dirLRU.getLRUFileIterator(); 
		INodeFile downgradeFile = null;	   
		while (LRUfiles.hasNext()) {
			downgradeFile = LRUfiles.next();
			// return the file if it has a replica in memory and it is present in the list of cold files to downgrade
			if (ReplicationVector.GetReplication(StorageType.MEMORY,downgradeFile.getReplicationVector()) > 0 && coldFiles.contains(downgradeFile.getParent().getFullPathName()))
				return downgradeFile;
		}
		return null;
	}

	@Override
	protected void onFileInfoUpdate(INodeFile file) {
		if (dirLRU.containsFile(file))
			dirLRU.accessFile(file);
		else{
			if (ReplicationVector.GetReplication(StorageType.MEMORY,file.getReplicationVector()) > 0)
				dirLRU.addFile(file); // add the file if it has a replica in memory and it is not present in the LRU list
		}      
	}

	@Override
	protected void onFileDelete(INodeFile file) {
		dirLRU.deleteFile(file);
	}
}
