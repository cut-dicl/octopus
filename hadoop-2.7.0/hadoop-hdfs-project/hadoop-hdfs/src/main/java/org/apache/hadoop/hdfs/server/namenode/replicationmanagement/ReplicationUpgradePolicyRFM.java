package org.apache.hadoop.hdfs.server.namenode.replicationmanagement;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStatistics;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;

/**
 * An upgrade policy based on RFM. See {@link RFMCollection} for details.
 * 
 * @author herodotos.herodotou
 */
public class ReplicationUpgradePolicyRFM extends
      ReplicationUpgradePolicyDefault {

   private RFMCollection rfm;
   private boolean rfmOwner;
   private float topPercent;
   
   private static int MAX_COUNT = 100;

   @Override
   protected void initialize(Configuration conf, FSDirectory dir,
         DatanodeStatistics stats) {
      super.initialize(conf, dir, stats);

      topPercent = 0.01f * conf.getFloat(
            DFSConfigKeys.DFS_REPLICATION_UPGRADE_POLICY_RFM_TOP_PERCENT_KEY,
            DFSConfigKeys.DFS_REPLICATION_UPGRADE_POLICY_RFM_TOP_PERCENT_DEFAULT);

      rfm = RFMCollection.getSharedRFM();
      rfmOwner = (RFMCollection.getSharedCount() == 1);
      if (rfmOwner) {
         ArrayList<INodeFile> fileList = ReplicationManagementUtils
               .collectSortedFiles(dir);
         for (INodeFile file : fileList)
            rfm.addFile(file);
      }
   }

   @Override
   protected boolean triggerUpgrade(StorageType type, INodeFile file) {
      if (file != null && stats.containsStorageTier(type)) {
         int topFiles = Math.min(MAX_COUNT, Math.round(topPercent * rfm.size()));
         int count = 0;
         Iterator<INodeFile> files = rfm.descFileIter();
         while (files.hasNext() && count < topFiles) {
            if (files.next().equals(file))
               return true;
            ++count;
         }
      }
      
      return false;
   }

   @Override
   protected void onFileInfoUpdate(INodeFile file) {
      if (rfmOwner && file != null) {
         if (rfm.containsFile(file))
            rfm.accessFile(file);
         else
            rfm.addFile(file);
      }
   }

   @Override
   protected void onFileDelete(INodeFile file) {
      if (rfmOwner && file != null)
         rfm.removeFile(file);
   }

}
