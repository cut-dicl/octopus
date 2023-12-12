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
 * This class includes a policy to select a file upgrade based on the number of
 * accesses in the recent past
 * 
 * @author elena.kakoulli
 */
public class ReplicationUpgradePolicyAccessBased extends
      ReplicationUpgradePolicyDefault {

   private short countThreshold;
   private long timeThreshold;
   private Map<Long, AccessStats> filesMap;

   @Override
   protected void initialize(Configuration conf, FSDirectory directory,
         DatanodeStatistics stats) {
      super.initialize(conf, dir, stats);

      countThreshold = (short) conf.getInt(
            DFSConfigKeys.DFS_REPLICATION_UPGRADE_POLICY_ACCESS_COUNT_KEY,
            DFSConfigKeys.DFS_REPLICATION_UPGRADE_POLICY_ACCESS_COUNT_DEFAULT);
      if (countThreshold < 1)
         countThreshold = 1;

      timeThreshold = 60l * 60 * 1000 * conf.getInt(
            DFSConfigKeys.DFS_REPLICATION_UPGRADE_POLICY_ACCESS_TIME_KEY,
            DFSConfigKeys.DFS_REPLICATION_UPGRADE_POLICY_ACCESS_TIME_DEFAULT);

      filesMap = new HashMap<Long, AccessStats>();
      Iterator<INodeWithAdditionalFields> iter = directory.getINodeMap().getMapIterator();
      while (iter.hasNext()) {
         INodeWithAdditionalFields n = iter.next();
         if (n.isFile() && !n.asFile().isUnderConstruction())
            filesMap.put(n.asFile().getId(), 
                  new AccessStats(countThreshold, n.asFile().getAccessTime()));
      }
   }

   @Override
   protected boolean triggerUpgrade(StorageType type, INodeFile file) {
      // trigger upgrade when the number of accesses in the desired time
      // interval is above the threshold
      if (file != null 
            && stats.containsStorageTier(type) 
            && filesMap.containsKey(file.getId())) {
         AccessStats stats = filesMap.get(file.getId());
         return stats.getCount() == countThreshold 
               && stats.getOldestTime() > System.currentTimeMillis() - timeThreshold;
      }

      return false;
   }

   @Override
   protected void onFileInfoUpdate(INodeFile file) {
      if (filesMap.containsKey(file.getId())) {
         filesMap.get(file.getId()).addAccessTime(System.currentTimeMillis());
      } else { // put the new file in filesMap
         filesMap.put(file.getId(), 
               new AccessStats(countThreshold, System.currentTimeMillis()));
      }
   }

   @Override
   protected void onFileDelete(INodeFile file) {
      filesMap.remove(file.getId());
   }

   private class AccessStats {
      private short head; // most recent access index
      private short count; // num of accesses
      private long[] accessTimes;
      
      AccessStats(short countThreshold, long accessTime) {
         this.head = 0;
         this.count = 1;
         this.accessTimes = new long[countThreshold];
         this.accessTimes[0] = accessTime;
      }

      public void addAccessTime(long accessTime) {
         head = (short) ((head + 1) % accessTimes.length);
         accessTimes[head] = accessTime;
         if (count < accessTimes.length)
            ++count;
      }

      public long getOldestTime() {
         if (count < accessTimes.length)
            return accessTimes[0];
         else
            return accessTimes[(head + 1) % accessTimes.length];
      }

      public short getCount() {
         return count;
      }
   }
}
