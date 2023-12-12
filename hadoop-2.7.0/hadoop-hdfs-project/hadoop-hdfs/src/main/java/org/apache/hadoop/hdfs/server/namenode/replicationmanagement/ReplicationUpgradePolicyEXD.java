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

import com.google.common.annotations.VisibleForTesting;

/**
 * This class implements the Exponential Decay (EXD) algorithm that explores the
 * tradeoff between recency and frequency.
 * 
 * Weight = 1 + [e^(-alpha * (time_now - time_last_access)] * Weight
 * 
 * @author herodotos.herodotou
 */
public class ReplicationUpgradePolicyEXD
      extends ReplicationUpgradePolicyDefault {

   private SortedWeightedTree<WeightedEXDNode> weightedTree;
   
   @VisibleForTesting
   public static float Threshold = 0.2f;

   @Override
   protected void initialize(Configuration conf, FSDirectory dir,
         DatanodeStatistics stats) {
      super.initialize(conf, dir, stats);
      
      WeightedEXDNode.ALPHA = 1d / (60d * 60 * 1000 * conf.getInt(
            DFSConfigKeys.DFS_REPLICATION_POLICY_WEIGHTED_BIAS_HOURS_KEY,
            DFSConfigKeys.DFS_REPLICATION_POLICY_WEIGHTED_BIAS_HOURS_DEFAULT));
      
      Threshold = conf.getFloat(
            DFSConfigKeys.DFS_REPLICATION_DOWNGRADE_POLICY_TRIGGER_THRESHOLD_KEY,
            DFSConfigKeys.DFS_REPLICATION_DOWNGRADE_POLICY_TRIGGER_THRESHOLD_DEFAULT);
      Threshold = (100 - Threshold) / 100;

      this.weightedTree = new SortedWeightedTree<WeightedEXDNode>();
      Iterator<INodeWithAdditionalFields> iter = dir.getINodeMap()
            .getMapIterator();
      while (iter.hasNext()) {
         INodeWithAdditionalFields n = iter.next();
         if (n.isFile() && !n.asFile().isUnderConstruction()) {
            weightedTree.addNode(new WeightedEXDNode(n.asFile()));
         }
      }
   }

   @Override
   protected boolean triggerUpgrade(StorageType type, INodeFile file) {
      if (file == null || !weightedTree.existWeightedNode(file.getId())
            || !stats.containsStorageTier(type))
         return false;

      long oldRepl = file.getReplicationVector();
      long newRepl = ReplicationManagementUtils.selectOneTierUp(file, type,
            stats);
      if (oldRepl == newRepl)
         return false;

      ReplicationVector newRV = new ReplicationVector(newRepl);
      newRV.subtractReplicationVector(new ReplicationVector(oldRepl));
      for (StorageType upType : StorageType.valuesOrdered()) {
         if (newRV.getReplication(upType) > 0) {
            long totalSpace = stats.getCapacityTotal(upType);
            long freeSpace = stats.getCapacityRemaining(upType);
            long currSize = file.computeFileSize();

            freeSpace = freeSpace - (long) (Threshold * totalSpace);
            if (freeSpace < 0)
               freeSpace = 0l;
            
            // If there is enough space, it is ok to upgrade
            if (freeSpace >= currSize)
               return true;
            
            // Compute the weight of the files that will need to be downgraded
            // if we decide to upgrade this file
            double sumWeights = 0d;
            WeightedNode currNode = weightedTree.getNode(file.getId());
            Iterator<WeightedNode> nodes = weightedTree.ascIter();
            while (nodes.hasNext()) {
               WeightedNode nextNode = nodes.next();
               if (sumWeights + nextNode.getWeight() < currNode.getWeight()) {
                  sumWeights += nextNode.getWeight();
                  freeSpace += nextNode.getFile().computeFileSize();
                  if (freeSpace >= currSize)
                     break; // we found enough files that could be replaced
               } else {
                  break; // we will not find any more files
               }
            }

            return (freeSpace >= currSize);
         }
      }

      return false;
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
