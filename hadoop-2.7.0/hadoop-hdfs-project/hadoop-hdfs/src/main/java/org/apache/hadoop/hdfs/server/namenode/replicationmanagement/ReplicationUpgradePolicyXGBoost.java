package org.apache.hadoop.hdfs.server.namenode.replicationmanagement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ReplicationVector;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStatistics;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An upgrade policy based on XGBoost. See {@link XGBoostPredictor} for details.
 * 
 * @author herodotos.herodotou
 */
public class ReplicationUpgradePolicyXGBoost
      extends ReplicationUpgradePolicyDefault {

   public static final Logger LOG = LoggerFactory
         .getLogger(ReplicationUpgradePolicyXGBoost.class);

   private XGBoostPredictor xgbPredictor = null;
   private FileHistoryCollection fhCollection;
   private boolean fhcOwner;

   private int topK;
   private double threshold;

   @Override
   protected void initialize(Configuration conf, FSDirectory dir,
         DatanodeStatistics stats) {
      super.initialize(conf, dir, stats);

      // Get configuration parameters
      topK = conf.getInt(
            DFSConfigKeys.DFS_REPLICATION_UPGRADE_POLICY_XGBOOST_TOP_K_KEY,
            DFSConfigKeys.DFS_REPLICATION_UPGRADE_POLICY_XGBOOST_TOP_K_DEF);
      threshold = conf.getFloat(
            DFSConfigKeys.DFS_REPLICATION_UPGRADE_POLICY_XGBOOST_DISCR_THRESHOLD_KEY,
            DFSConfigKeys.DFS_REPLICATION_UPGRADE_POLICY_XGBOOST_DISCR_THRESHOLD_DEF);

      int maxNumAccesses = conf.getInt(
            DFSConfigKeys.DFS_REPLICATION_XGBOOST_MAX_NUM_ACCESSES_KEY,
            DFSConfigKeys.DFS_REPLICATION_XGBOOST_MAX_NUM_ACCESSES_DEF);
      String modelFilePath = conf.get(
            DFSConfigKeys.DFS_REPLICATION_UPGRADE_POLICY_XGBOOST_MODEL_FILE_PATH_KEY,
            DFSConfigKeys.DFS_REPLICATION_UPGRADE_POLICY_XGBOOST_MODEL_FILE_PATH_DEF);

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
   }

   // Data members maintained across function calls
   private Iterator<INodeFile> currIter;
   private INodeFile currFile;
   private int currCounter;
   private long currNow;
   private double currProb;

   @Override
   protected boolean triggerUpgrade(StorageType type, INodeFile file) {
      if (xgbPredictor != null && stats.containsStorageTier(type)) {
         currIter = fhCollection.getMRUFileIterator();
         currCounter = 0;
         currNow = System.currentTimeMillis();
         currProb = 0f;

         // Iterate over recent files and compute their access probabilities
         INodeFile f;
         while (currCounter < topK && currIter.hasNext()) {
            f = currIter.next();

            if (f.getReplicationFactor(type) > 0 && !f.isUnderConstruction()) {
               currProb = xgbPredictor.predictAccessProb(
                     fhCollection.getFileHistory(f), currNow); 
               if (currProb > threshold) {
                  currFile = f;
                  return true;
               }
            }
            ++currCounter;
         }
      }

      // No file found for upgrade
      currIter = null;
      currFile = null;

      return false;
   }

   @Override
   protected boolean continueUpgrade(StorageType type) {
      if (xgbPredictor != null && stats.containsStorageTier(type)
            && currIter != null) {
         // Continue from where triggerUpgrade() stopped
         INodeFile f;
         while (currCounter < topK && currIter.hasNext()) {
            f = currIter.next();

            if (f.getReplicationFactor(type) > 0 && !f.isUnderConstruction()) {
               currProb = xgbPredictor.predictAccessProb(
                     fhCollection.getFileHistory(f), currNow); 
               if (currProb > threshold) {
                  currFile = f;
                  return true;
               }
            }
            ++currCounter;
         }
      }

      // No file found for upgrade
      currIter = null;
      currFile = null;

      return false;
   }

   @Override
   protected INodeFile selectFileToUpgrade(INodeFile file, StorageType type) {
      return currFile;
   }

   @Override
   protected long upgradeFile(INodeFile file, StorageType type) {
      long oldRepl = file.getReplicationVector();

      if (currProb > 2 * threshold) {
         // High probability --> move a copy up to memory or other tier
         long newRepl = ReplicationManagementUtils.selectMemoryTier(file, type, stats);
         if (oldRepl != newRepl)
            return newRepl;
         else
            return ReplicationManagementUtils.selectOneTierUp(file, type, stats);
      } else {
         // Move a copy up to memory, if not there already
         if (ReplicationVector.GetReplication(StorageType.MEMORY, oldRepl) == 0)
            return ReplicationManagementUtils.selectMemoryTier(file, type, stats);
         else
            return oldRepl;
      }
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
