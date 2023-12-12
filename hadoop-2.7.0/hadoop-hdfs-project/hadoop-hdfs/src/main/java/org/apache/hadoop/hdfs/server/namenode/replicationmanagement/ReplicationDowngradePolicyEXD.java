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
 * This class implements the Exponential Decay (EXD) algorithm that 
 * explores the tradeoff between recency and frequency.
 * 
 * Weight = 1 + [e^(-alpha * (time_now - time_last_access)] * Weight
 * 
 * @author herodotos.herodotou
 */

public class ReplicationDowngradePolicyEXD extends
      ReplicationDowngradePolicyThreshold {

   private SortedWeightedTree<WeightedEXDNode> weightedTree;
   
   @Override
   protected void initialize(Configuration conf, FSDirectory dir, DatanodeStatistics stats) {
      super.initialize(conf, dir, stats);
      WeightedEXDNode.ALPHA = 1d / (60d * 60 * 1000 * conf.getInt(
           DFSConfigKeys.DFS_REPLICATION_POLICY_WEIGHTED_BIAS_HOURS_KEY,
           DFSConfigKeys.DFS_REPLICATION_POLICY_WEIGHTED_BIAS_HOURS_DEFAULT));

      this.weightedTree = new SortedWeightedTree<WeightedEXDNode>();
      Iterator<INodeWithAdditionalFields> iter = dir.getINodeMap().getMapIterator();
      while (iter.hasNext()) {
         INodeWithAdditionalFields n = iter.next();
         if (n.isFile()&& !n.asFile().isUnderConstruction()) {
            weightedTree.addNode(new WeightedEXDNode(n.asFile()));
         }
      }
   }

   @Override
   protected INodeFile selectFileToDowngrade(StorageType type) {
      Iterator<INodeFile> files = weightedTree.ascFileIter();
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
         if (weightedTree.existWeightedNode(file.getId()))
            weightedTree.updateNode(file);
         else {
            weightedTree.addNode(new WeightedEXDNode(file));
         }
      }
   }

   @Override
   protected void onFileDelete(INodeFile file) {
      if (file != null)
         weightedTree.deleteNode(file);
   }

}
