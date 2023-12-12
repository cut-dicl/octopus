package org.apache.hadoop.hdfs.server.namenode.replicationmanagement;

import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ReplicationVector;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStatistics;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeWithAdditionalFields;

/**
 * This class includes the LFU (Least Frequently Used) policy to select a file
 * for downgrade.
 * 
 * 
 * @author elena.kakoulli
 */

public class ReplicationDowngradePolicyLFU extends
      ReplicationDowngradePolicyThreshold {

   protected SortedWeightedTree<WeightedLFUNode> dirLFU;

   @Override
   protected void initialize(Configuration conf, FSDirectory dir,
         DatanodeStatistics statistics) {
      super.initialize(conf, dir, statistics);
      this.dirLFU = new SortedWeightedTree<WeightedLFUNode>();
      Iterator<INodeWithAdditionalFields> iter = dir.getINodeMap().getMapIterator();
      while (iter.hasNext()) {
         INodeWithAdditionalFields n = iter.next();
         if (n.isFile()) {
            WeightedLFUNode newNode = new WeightedLFUNode(n.asFile());
            dirLFU.addNode(newNode);
         }
      }
   }

   @Override
   protected INodeFile selectFileToDowngrade(StorageType type) {
      Iterator<INodeFile> files = dirLFU.ascFileIter();
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
         if (dirLFU.existWeightedNode(file.getId()))
            dirLFU.updateNode(file);
         else {
            WeightedLFUNode newNode = new WeightedLFUNode(file);
            dirLFU.addNode(newNode);
         }
      }
   }

   @Override
   protected void onFileDelete(INodeFile file) {
      if (file != null)
         dirLFU.deleteNode(file);
   }
}
