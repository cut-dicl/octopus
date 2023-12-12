package org.apache.hadoop.hdfs.server.namenode.replicationmanagement;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.replicationmanagement.FileHistoryCollection.FileHistory;

import biz.k11i.xgboost.Predictor;
import biz.k11i.xgboost.util.FVec;

/**
 * XGBoost (eXtreme Gradient Boosting) implements machine learning algorithms
 * under the Gradient Boosting framework (https://github.com/dmlc/xgboost/),
 * which produces a prediction model in the form of an ensemble of weak
 * prediction models, typically decision trees. This class uses a pure Java
 * implementation of XGBoost predictor for online prediction tasks
 * (https://github.com/komiya-atsushi/xgboost-predictor-java).
 * 
 * @author herodotos.herodotou
 */
public class XGBoostPredictor {

   // Private data members
   private Predictor predictor = null;

   private long maxFileSize;
   private long maxCreationInterval;
   private long maxAccessInterval;
   private boolean relativeDelta;

   /**
    * Constructor
    */
   public XGBoostPredictor(Configuration conf, String modelFilePath)
         throws IOException {
      // Get config parameters
      maxFileSize = conf.getLongBytes(
            DFSConfigKeys.DFS_REPLICATION_XGBOOST_MAX_FILE_SIZE_KEY,
            DFSConfigKeys.DFS_REPLICATION_XGBOOST_MAX_FILE_SIZE_DEF);
      maxCreationInterval = conf.getLongBytes(
            DFSConfigKeys.DFS_REPLICATION_XGBOOST_MAX_CREATION_INTERVAL_SECS_KEY,
            DFSConfigKeys.DFS_REPLICATION_XGBOOST_MAX_CREATION_INTERVAL_SECS_DEF);
      maxAccessInterval = conf.getLongBytes(
            DFSConfigKeys.DFS_REPLICATION_XGBOOST_MAX_ACCESS_INTERVAL_SECS_KEY,
            DFSConfigKeys.DFS_REPLICATION_XGBOOST_MAX_ACCESS_INTERVAL_SECS_DEF);
      relativeDelta = conf.getBoolean(
            DFSConfigKeys.DFS_REPLICATION_XGBOOST_RELATIVE_DELTA_KEY,
            DFSConfigKeys.DFS_REPLICATION_XGBOOST_RELATIVE_DELTA_DEF);

      predictor = new Predictor(new java.io.FileInputStream(modelFilePath));
   }

   /**
    * Computes the probability the input file will get accessed in the near
    * future, starting from 'now'
    * 
    * Returns -1 if unable to predict the access probability.
    * 
    * @param file
    * @return
    */
   public double predictAccessProb(FileHistory history, long now) {
      if (history == null || predictor == null)
         return -1d;

      HashMap<Integer, Float> features = new HashMap<Integer, Float>();

      // Compute file size feature
      float fileSizeFeature = computeFileSizeFeature(history.getFileSize(),
            maxFileSize);
      if (fileSizeFeature >= 0)
         features.put(0, fileSizeFeature);

      // Compute creation time feature
      float creationTimeFeature = computeDeltaTimeFeature(now,
            history.getCreationTime(), maxCreationInterval);
      if (creationTimeFeature >= 0)
         features.put(1, creationTimeFeature);

      // Compute access time features (in reverse chronological order)
      if (history.getNumAccesses() > 0) {
         int indexF = 0;
         int indexA = history.getNumAccesses() - 1;
         long previous = now;
         float accessTimeFeature;

         while (indexA >= 0) {
            accessTimeFeature = computeDeltaTimeFeature(previous,
                  history.getAccessTime(indexA), maxAccessInterval);
            if (accessTimeFeature >= 0)
               features.put(2 + indexF, accessTimeFeature);
            if (relativeDelta)
               previous = history.getAccessTime(indexA);
            ++indexF;
            --indexA;
         }
         
         if (relativeDelta) {
            // delta from oldest access to creation
            accessTimeFeature = computeDeltaTimeFeature(previous,
                  history.getCreationTime(), maxAccessInterval);
            if (accessTimeFeature >= 0)
               features.put(2 + indexF, accessTimeFeature);
         }
      }

      FVec fVector = FVec.Transformer.fromMap(features);
      double[] prediction = predictor.predict(fVector);

      return prediction[0];
   }

   /**
    * Compute file size feature by normalizing based on the max file size.
    * 
    * @param fileSize
    * @param maxFileSize
    * @return
    */
   private float computeFileSizeFeature(long fileSize, long maxFileSize) {
      float fileSizeFeature = -1f;
      if (maxFileSize > 0) {
         if (fileSize > maxFileSize)
            fileSizeFeature = 1f;
         else
            fileSizeFeature = (float) Math
                  .sqrt(fileSize / (double) maxFileSize);
      }

      return fileSizeFeature;
   }

   /**
    * Compute creation or access time feature by normalizing based on the max
    * creation interval.
    * 
    * @param now
    * @param pastTime
    * @param maxDeltaInterval
    * @return
    */
   private float computeDeltaTimeFeature(long now, long pastTime,
         long maxDeltaInterval) {
      float feature = -1f;

      if (maxDeltaInterval > 0) {
         long interval = now - pastTime;
         if (interval > maxDeltaInterval)
            feature = 1f;
         else
            feature = (float) Math.sqrt(interval / (double) maxDeltaInterval);
      }

      return feature;
   }

}
