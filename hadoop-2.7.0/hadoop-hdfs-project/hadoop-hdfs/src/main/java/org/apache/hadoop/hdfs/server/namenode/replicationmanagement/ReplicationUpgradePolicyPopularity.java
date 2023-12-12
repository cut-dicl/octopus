package org.apache.hadoop.hdfs.server.namenode.replicationmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStatistics;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;

/**
 * This policy computes a popularity score (weight) for each file that is based
 * on count and time of accesses and keeps track of basic distribution metrics
 * (mean, stdev). If a file's popularity becomes larger than two times the stdev
 * away from the mean, then it is considered for upgrade.
 * 
 * @author herodotos.herodotou@cut.ac.cy
 */
public class ReplicationUpgradePolicyPopularity extends
      ReplicationUpgradePolicyWeighted {

   private long count = 0l; // number of weights (files)
   private double mean = 0d; // mean of the weights
   private double lamda = 0d; // mean of the squared weights
   private double stdev = 0d; // standard deviation of weights

   @Override
   protected void initialize(Configuration conf, FSDirectory dir,
         DatanodeStatistics stats) {
      super.initialize(conf, dir, stats);

      // Compute the initial stats
      count = filesMap.size();
      if (count > 0) {
         double sum = 0d;
         double sum2 = 0d;
         for (WeightedAccessNode node : filesMap.values()) {
            sum += node.getWeight();
            sum2 += node.getWeight() * node.getWeight();
         }
         
         mean = sum / count;
         lamda = sum2 / count;
         stdev = Math.sqrt(lamda - (mean * mean)); // Konig-Huygens
      }
   }

   @Override
   protected boolean triggerUpgrade(StorageType type, INodeFile file) {
      // trigger upgrade when the weight is above the threshold
      if (file != null 
            && stats.containsStorageTier(type) 
            && count >= 5
            && filesMap.containsKey(file.getId())
            && filesMap.get(file.getId()).getWeight() >= mean + 2 * stdev) {
         return true;
      }

      return false;
   }

   @Override
   protected void onFileInfoUpdate(INodeFile file) {
      if (filesMap.containsKey(file.getId())) { 
         // update weight
         WeightedAccessNode node = filesMap.get(file.getId());
         double oldWeight = node.getWeight();
         node.updateWeight();
         double newWeight = node.getWeight();

         // update stats
         mean = (count * mean + newWeight - oldWeight) / count;
         lamda = (count * lamda + newWeight * newWeight - oldWeight * oldWeight) / count;
         stdev = Math.sqrt(lamda - (mean * mean));         
      } else { 
         // put the new file in filesMap
         WeightedAccessNode node = new WeightedAccessNode(file);
         filesMap.put(file.getId(), node);
         
         // inrease stats
         mean = (count * mean + node.getWeight()) / (count + 1);
         lamda = (count * lamda + node.getWeight() * node.getWeight()) / (count + 1);
         count += 1;
         stdev = Math.sqrt(lamda - (mean * mean));
      }
   }

   @Override
   protected void onFileDelete(INodeFile file) {
      WeightedAccessNode node = filesMap.remove(file.getId());
      
      // decrease stats
      if (count > 1) {
         mean = (count * mean - node.getWeight()) / (count - 1);
         lamda = (count * lamda - node.getWeight() * node.getWeight()) / (count - 1);
         count -= 1;
         stdev = Math.sqrt(lamda - (mean * mean));
      } else {
         count = 0l;
         mean = lamda = stdev = 0d;
      }
   }

}
