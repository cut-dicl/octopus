package org.apache.hadoop.hdfs.server.namenode.replicationmanagement;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStatistics;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;

/**
 * This class includes the LRU (Least Recently Used) policy to select a file for
 * downgrade.
 * 
 * 
 * @author elena.kakoulli
 */
public class ReplicationDowngradePolicyLRU extends
      ReplicationDowngradePolicyThreshold {

   protected AccessBasedList dirLRU;

   @Override
   protected void initialize(Configuration conf, FSDirectory dir,
         DatanodeStatistics statistics) {
      super.initialize(conf, dir, statistics);

      this.dirLRU = new AccessBasedList();
      ArrayList<INodeFile> fileList = ReplicationManagementUtils.collectSortedFiles(dir);
      for (INodeFile file : fileList)
         dirLRU.addFile(file);
   }

   @Override
   protected INodeFile selectFileToDowngrade(StorageType type) {
      return dirLRU.getLRUFile(type);
   }

   @Override
   protected void onFileInfoUpdate(INodeFile file) {
      if (dirLRU.containsFile(file))
         dirLRU.accessFile(file);
      else
         dirLRU.addFile(file);
   }

   @Override
   protected void onFileDelete(INodeFile file) {
      dirLRU.deleteFile(file);
   }
}
