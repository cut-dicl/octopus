package org.apache.hadoop.hdfs.server.namenode.replicationmanagement;

import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ReplicationVector;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStatistics;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeWithAdditionalFields;

/**
 * This class includes the Weighted (based on both number and time of accesses)
 * policy to select a file for downgrade.
 * 
 * @author elena.kakoulli
 */

public class ReplicationDowngradePolicyWeighted extends
      ReplicationDowngradePolicyThreshold {

   protected SortedWeightedTree<WeightedAccessNode> dirWeighted;
   
   @Override
   protected void initialize(Configuration conf, FSDirectory dir, DatanodeStatistics stats) {
      super.initialize(conf, dir, stats);
      WeightedAccessNode.BIAS = 60d * 60 * 1000 * conf.getInt(
           DFSConfigKeys.DFS_REPLICATION_POLICY_WEIGHTED_BIAS_HOURS_KEY,
           DFSConfigKeys.DFS_REPLICATION_POLICY_WEIGHTED_BIAS_HOURS_DEFAULT);

      this.dirWeighted = new SortedWeightedTree<WeightedAccessNode>();
      Iterator<INodeWithAdditionalFields> iter = dir.getINodeMap().getMapIterator();
      while (iter.hasNext()) {
         INodeWithAdditionalFields n = iter.next();
         if (n.isFile()) {
            dirWeighted.addNode(new WeightedAccessNode(n.asFile()));
         }
      }
   }

   @Override
   protected INodeFile selectFileToDowngrade(StorageType type) {
      Iterator<INodeFile> files = dirWeighted.ascFileIter();
      while (files.hasNext()) {
         INodeFile file = files.next();
         if (ReplicationVector.GetReplication(type, file.getReplicationVector()) > 0
               && !file.isUnderConstruction()) {
            return file;
         }
      }
      return null;
   }

   @Override
   protected void onFileInfoUpdate(INodeFile file) {
      if (file != null) {
         if (dirWeighted.existWeightedNode(file.getId()))
            dirWeighted.updateNode(file);
         else {
            dirWeighted.addNode(new WeightedAccessNode(file));
         }
      }
   }

   @Override
   protected void onFileDelete(INodeFile file) {
      if (file != null)
         dirWeighted.deleteNode(file);
   }

}
