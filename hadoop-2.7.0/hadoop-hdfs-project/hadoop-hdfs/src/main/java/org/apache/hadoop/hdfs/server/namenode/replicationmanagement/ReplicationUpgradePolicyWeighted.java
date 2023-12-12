package org.apache.hadoop.hdfs.server.namenode.replicationmanagement;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStatistics;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeWithAdditionalFields;

/**
 * This class includes the Weighted (based on a file's weight - #accesses)
 * policy to select a file for upgrade.
 *
 * 
 * @author elena.kakoulli
 */
public class ReplicationUpgradePolicyWeighted extends
      ReplicationUpgradePolicyDefault {

   private double weightThreshold;
   protected Map<Long, WeightedAccessNode> filesMap;

   @Override
   protected void initialize(Configuration conf, FSDirectory dir,
         DatanodeStatistics stats) {
      super.initialize(conf, dir, stats);
      WeightedAccessNode.BIAS = 60d * 60 * 1000 * conf.getLong(
            DFSConfigKeys.DFS_REPLICATION_POLICY_WEIGHTED_BIAS_HOURS_KEY,
            DFSConfigKeys.DFS_REPLICATION_POLICY_WEIGHTED_BIAS_HOURS_DEFAULT);
      this.weightThreshold = conf.getDouble(
            DFSConfigKeys.DFS_REPLICATION_UPGRADE_POLICY_WEIGHT_THRESHOLD_KEY,
            DFSConfigKeys.DFS_REPLICATION_UPGRADE_POLICY_WEIGHT_THRESHOLD_DEFAULT);

      this.filesMap = new HashMap<Long, WeightedAccessNode>();
      Iterator<INodeWithAdditionalFields> iter = dir.getINodeMap().getMapIterator();
      while (iter.hasNext()) {
         INodeWithAdditionalFields n = iter.next();
         if (n.isFile() && !n.asFile().isUnderConstruction())
            filesMap.put(n.asFile().getId(), new WeightedAccessNode(n.asFile()));
      }
   }

   @Override
   protected boolean triggerUpgrade(StorageType type, INodeFile file) {
      // trigger upgrade when the weight is above the threshold
      if (file != null 
            && stats.containsStorageTier(type)
            && filesMap.containsKey(file.getId())
            && filesMap.get(file.getId()).getWeight() >= weightThreshold) {
         return true;
      }

      return false;
   }

   @Override
   protected void onFileInfoUpdate(INodeFile file) {
      if (filesMap.containsKey(file.getId())) { // update weight
         filesMap.get(file.getId()).updateWeight();
      } else { // put the new file in filesMap
         filesMap.put(file.getId(), new WeightedAccessNode(file));
      }
   }

   @Override
   protected void onFileDelete(INodeFile file) {
      filesMap.remove(file.getId());
   }

}
