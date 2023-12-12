package org.apache.hadoop.hdfs.server.namenode.replicationmanagement;

import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStatistics;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;

/**
 * This class includes the dynamic policy (based on MOOP) to select a file for
 * downgrade among the top-k files based on frequency
 * 
 * @author elena.kakoulli
 */
public class ReplicationDowngradePolicyDynamicLFU extends
      ReplicationDowngradePolicyLFU {

   private DynamicPolicyHelper helper;
   private int topk;

   @Override
   protected void initialize(Configuration conf, FSDirectory dir,
         DatanodeStatistics stats) {
      super.initialize(conf, dir, stats);

      helper = new DynamicPolicyHelper(conf, stats);

      topk = conf.getInt(
               DFSConfigKeys.DFS_REPLICATION_DOWNGRADE_POLICY_DYNAMIC_TOP_K_KEY,
               DFSConfigKeys.DFS_REPLICATION_DOWNGRADE_POLICY_DYNAMIC_TOP_K_DEFAULT);
   }

   @Override
   protected INodeFile selectFileToDowngrade(StorageType type) {
      int maxFiles = Math.min((dirLFU.size() + 1) / 2, topk);
      Iterator<INodeFile> files = dirLFU.ascFileIter();

      return helper.selectFileToDowngrade(type, files, maxFiles);
   }

   @Override
   protected long downgradeFile(INodeFile file, StorageType type) {
      return helper.getNewReplVector(file, type);
   }
}
