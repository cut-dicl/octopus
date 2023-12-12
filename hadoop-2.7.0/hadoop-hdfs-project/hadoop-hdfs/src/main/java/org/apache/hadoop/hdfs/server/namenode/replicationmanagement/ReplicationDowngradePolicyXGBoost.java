package org.apache.hadoop.hdfs.server.namenode.replicationmanagement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStatistics;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;

/**
 * A downgrade policy based on XGBoost. See {@link XGBoostPredictor} for
 * details.
 * 
 * @author herodotos.herodotou
 */
public class ReplicationDowngradePolicyXGBoost
      extends ReplicationDowngradePolicyThreshold {

   private XGBoostPredictor xgbPredictor = null;
   private FileHistoryCollection fhCollection;
   private boolean fhcOwner;

   private DynamicPolicyHelper helper;
   
   private int topK;
   private double threshold;

   @Override
   protected void initialize(Configuration conf, FSDirectory dir,
         DatanodeStatistics stats) {
      super.initialize(conf, dir, stats);

      // Get configuration parameters
      topK = conf.getInt(
            DFSConfigKeys.DFS_REPLICATION_DOWNGRADE_POLICY_XGBOOST_TOP_K_KEY,
            DFSConfigKeys.DFS_REPLICATION_DOWNGRADE_POLICY_XGBOOST_TOP_K_DEF);
      threshold = conf.getFloat(
            DFSConfigKeys.DFS_REPLICATION_DOWNGRADE_POLICY_XGBOOST_DISCR_THRESHOLD_KEY,
            DFSConfigKeys.DFS_REPLICATION_DOWNGRADE_POLICY_XGBOOST_DISCR_THRESHOLD_DEF);

      int maxNumAccesses = conf.getInt(
            DFSConfigKeys.DFS_REPLICATION_XGBOOST_MAX_NUM_ACCESSES_KEY,
            DFSConfigKeys.DFS_REPLICATION_XGBOOST_MAX_NUM_ACCESSES_DEF);
      String modelFilePath = conf.get(
            DFSConfigKeys.DFS_REPLICATION_DOWNGRADE_POLICY_XGBOOST_MODEL_FILE_PATH_KEY,
            DFSConfigKeys.DFS_REPLICATION_DOWNGRADE_POLICY_XGBOOST_MODEL_FILE_PATH_DEF);

      try {
         xgbPredictor = new XGBoostPredictor(conf, modelFilePath);
         LOG.info("Successfully loaded XGBoost model from " + modelFilePath);
      } catch (IOException e) {
         LOG.error("ERROR: Exception reading the XGBoost model file", e);
      }

      fhCollection = FileHistoryCollection.getInstance(maxNumAccesses);
      fhcOwner = (FileHistoryCollection.getSharedCount() == 1);
      if (fhcOwner) {
         ArrayList<INodeFile> fileList = ReplicationManagementUtils
               .collectSortedFiles(dir);
         for (INodeFile file : fileList)
            fhCollection.addFile(file);
      }
      
      helper = new DynamicPolicyHelper(conf, stats);
   }

   @Override
   protected INodeFile selectFileToDowngrade(StorageType type) {

      if (xgbPredictor == null) {
         return fhCollection.getLRUFile(type);
      }

      // Iterate over old files and compute their access probabilities
      INodeFile currFile, minFile = null;
      double currProb, minProb = Double.MAX_VALUE;
      int counter = 0;
      long now = System.currentTimeMillis();

      Iterator<INodeFile> iter = fhCollection.getLRUFileIterator();
      while (iter.hasNext()) {
         currFile = iter.next();

         if (currFile.getReplicationFactor(type) > 0
               && !currFile.isUnderConstruction()) {
            // Find file with smallest access probability
            currProb = xgbPredictor.predictAccessProb(
                  fhCollection.getFileHistory(currFile), now);
            if (currProb < minProb) {
               minProb = currProb;
               minFile = currFile;
            }
         }
         ++counter;

         // Rational: return file in top-k with smallest probability, if lower
         // than threshold. From top-k to 2*top-k, return first file with
         // probability lower than threshold. After 2*top-k, return first file
         // with any computed probability.
         if (counter > 2 * topK) {
            if (minFile != null)
               return minFile;
         } else if (counter > topK) {
            if (minProb < threshold)
               return minFile;
         }
      }

      return minFile;
   }

   @Override
   protected long downgradeFile(INodeFile file, StorageType type) {
       return helper.downgradeFile(file, type);
   }

   @Override
   protected void onFileInfoUpdate(INodeFile file) {
      if (fhcOwner && file != null) {
         if (fhCollection.containsFile(file))
            fhCollection.accessFile(file);
         else
            fhCollection.addFile(file);
      }
   }

   @Override
   protected void onFileDelete(INodeFile file) {
      if (fhcOwner && file != null)
         fhCollection.removeFile(file);
   }
}
