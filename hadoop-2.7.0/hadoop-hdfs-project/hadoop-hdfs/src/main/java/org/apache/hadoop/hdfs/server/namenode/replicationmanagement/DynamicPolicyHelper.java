package org.apache.hadoop.hdfs.server.namenode.replicationmanagement;

import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ReplicationVector;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStatistics;
import org.apache.hadoop.hdfs.server.blockmanagement.DynamicBlockStoragePolicy;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;

/**
 * A helper class for selecting a file to downgrade from a list of target files.
 * 
 * @author herodotos.herodotou
 */
public class DynamicPolicyHelper {

   private DatanodeStatistics stats;
   private DynamicBlockStoragePolicy policy;

   private INodeFile selectedFile;
   private long selectedRV;

   public DynamicPolicyHelper(Configuration conf, DatanodeStatistics stats) {
      this.stats = stats;
      this.policy = new DynamicBlockStoragePolicy(stats, conf);

      this.selectedFile = null;
      this.selectedRV = 0;
   }

   /**
    * Use the MOOP policy to find the best file to downgrade among the provided
    * files. It will compare up to maxFiles from this list of files.
    * 
    * @param type
    * @param files
    * @param maxFiles
    * @return
    */
   public INodeFile selectFileToDowngrade(StorageType type,
         Iterator<INodeFile> files, int maxFiles) {

      StorageType[] selectedDownTiers = ReplicationManagementUtils
            .selectedDownTiers(stats, type);

      if (selectedDownTiers.length == 0)
         return null;

      policy.setStorageTypes(selectedDownTiers);

      double bestScore = Double.MAX_VALUE;
      selectedFile = null;
      int count = 0;

      while (files.hasNext() && count < maxFiles) {
         INodeFile file = files.next();
         if (file.getReplicationFactor(type) > 0 && !file.isUnderConstruction()) {
            ++count;
            policy.setFileSize(file.computeFileSize());
            ReplicationVector repV = new ReplicationVector(
                  file.getReplicationVector());
            repV.decrReplication(type, (short) 1);
            repV.incrReplication(null, (short) 1);
            ReplicationVector resolveRV = policy.resolveReplicationVector(repV);
            if (policy.isResolved()) {
               if (bestScore > 1.001 * policy.getBestScore()) {
                  bestScore = policy.getBestScore();
                  selectedFile = file;
                  selectedRV = resolveRV.getVectorEncoding();
               }
            }
         }
      }

      return selectedFile;
   }

   /**
    * If {@link #selectFileToDowngrade(StorageType, Iterator, int)} has been
    * called before this method invocation, then the already-selected
    * replication vector will be returned. Otherwise,
    * {@link #downgradeFile(INodeFile, StorageType)} is invoked.
    * 
    * @param file
    * @param type
    * @return
    */
   public long getNewReplVector(INodeFile file, StorageType type) {
      if (selectedFile != null && selectedFile == file)
         return selectedRV;
      else
         return downgradeFile(file, type);
   }

   /**
    * Use the dynamic policy to select a downgrade location for the provided
    * file
    * 
    * @param file
    * @param type
    * @return
    */
   public long downgradeFile(INodeFile file, StorageType type) {

      if (file.getReplicationFactor(type) == 0 || file.isUnderConstruction())
         return file.getReplicationVector();

      StorageType[] selectedDownTiers = ReplicationManagementUtils
            .selectedDownTiers(stats, type);
      if (selectedDownTiers.length == 0)
         return file.getReplicationVector();

      // Use the policy to find new replica location
      policy.setStorageTypes(selectedDownTiers);
      policy.setFileSize(file.computeFileSize());
      ReplicationVector repV = new ReplicationVector(
            file.getReplicationVector());
      repV.decrReplication(type, (short) 1);
      repV.incrReplication(null, (short) 1);
      ReplicationVector resolveRV = policy.resolveReplicationVector(repV);
      if (policy.isResolved())
         return resolveRV.getVectorEncoding();
      else
         return ReplicationManagementUtils.selectOneTierDown(file, type, stats);
   }

}
