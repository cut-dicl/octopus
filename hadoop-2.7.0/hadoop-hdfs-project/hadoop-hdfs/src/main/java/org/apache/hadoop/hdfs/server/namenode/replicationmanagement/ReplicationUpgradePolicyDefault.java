package org.apache.hadoop.hdfs.server.namenode.replicationmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStatistics;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;

/**
 * This class includes the default policy to select a file upgrade, which does
 * nothing.
 * 
 * 
 * @author elena.kakoulli
 */
public class ReplicationUpgradePolicyDefault extends ReplicationUpgradePolicy {

   protected FSDirectory dir;
   protected DatanodeStatistics stats;

   @Override
   protected void initialize(Configuration conf, FSDirectory dir,
         DatanodeStatistics stats) {
      this.dir = dir;
      this.stats = stats;
   }

   @Override
   protected boolean triggerUpgrade(StorageType type, INodeFile file) {
      return false;
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
      return ReplicationManagementUtils.selectOneTierUp(file, type, stats);
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
