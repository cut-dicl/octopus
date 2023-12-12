package org.apache.hadoop.hdfs.server.namenode.replicationmanagement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.hadoop.fs.ReplicationVector;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStatistics;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeWithAdditionalFields;

/**
 * This class includes various methods for replication management
 * 
 * 
 * @author elena.kakoulli
 */

public class ReplicationManagementUtils {

   /**
    * Move one file replica from 'type' to the next available lower tier
    */
    public static long selectOneTierDown(INodeFile file, StorageType type, DatanodeStatistics stats) {
        if (!(isLowestTier(type, stats))) {

            ReplicationVector repV = new ReplicationVector(file.getReplicationVector());

            StorageType[] types = StorageType.valuesOrdered();
            for (int i = 0; i < types.length; i++) {
                if (types[i].equals(type)) { // only one tier down
                    repV.decrReplication(type, (short) 1);
                    int newType = i + 1;
                    // loop to find the first down tier exist
                    while (newType < types.length && (!isExistTier(types[newType], stats))) {
                        newType++;
                    }
                    // if the first down tier exist and has available space for
                    // the file
                    if (newType < types.length && isExistTier(types[newType], stats)
                            && (stats.getCapacityRemaining(types[newType]) >= file.computeFileSize())) {
                        repV.incrReplication(types[newType], (short) 1);
                        return repV.getVectorEncoding();
                    } else
                        return file.getReplicationVector();
                }
            }
        }
        return file.getReplicationVector();
    }

   /**
    * Move one file replica from 'type' to the next available higher tier
    */
   public static long selectOneTierUp(INodeFile file, StorageType type,
         DatanodeStatistics stats) {

      long fileRV = file.getReplicationVector();
      if (isHighestTier(type, stats))
         return fileRV;
      
      ReplicationVector repV = new ReplicationVector(fileRV);
      StorageType[] types = StorageType.valuesOrdered();
      for (int i = 0; i < types.length; i++) {
         if (types[i].equals(type)) { // only one tier up
            repV.decrReplication(type, (short) 1);
            int newType = i - 1;
            // loop to find the first up tier exist
            while (newType >= 0 && (!isExistTier(types[newType], stats))) {
               newType--;
            }
            // if the first up tier exist
            if (newType >= 0 && isExistTier(types[newType], stats)) {
               repV.incrReplication(types[newType], (short) 1);
               if (repV.getNonVolatileReplicationCount() == 0)
                  return fileRV;
               else
                  return repV.getVectorEncoding();
            } else
               return fileRV;
         }
      }
      return fileRV;
   }

   /**
    * Move one file replica from 'type' to the Memory tier
    */
   public static long selectMemoryTier(INodeFile file, StorageType type,
         DatanodeStatistics stats) {

      long fileRV = file.getReplicationVector();
      if (isHighestTier(type, stats))
         return fileRV;

      // Move one replica to the memory tier (if exists)
      if (isExistTier(StorageType.MEMORY, stats)) {
         ReplicationVector repV = new ReplicationVector(fileRV);
         repV.decrReplication(type, (short) 1);
         repV.incrReplication(StorageType.MEMORY, (short) 1);
         if (repV.getNonVolatileReplicationCount() > 0)
            return repV.getVectorEncoding();
      }

      // Not possible to move to memory
      return fileRV;
   }

    //Check if the tier is the highest from the exist tiers
    public static boolean isHighestTier(StorageType type, DatanodeStatistics stats) {
        StorageType[] types = StorageType.valuesOrdered();
        for (int i = 0; i < types.length; i++) {
            if (isExistTier(types[i], stats)) {
                if (types[i].equals(type))
                    return true;
                else
                    return false;
            }
        }
        return false;
    }

    //Check if the tier is the lowest from the exist tiers
    public static boolean isLowestTier(StorageType type, DatanodeStatistics stats) {
        StorageType[] types = StorageType.valuesOrdered();
        for (int i = types.length - 1; i >= 0; i--) {
            if (isExistTier(types[i], stats)) {
                if (types[i].equals(type))
                    return true;
                else
                    return false;
            }
        }
        return false;
    }

    /**
     * Get the highest tiers from the existing tiers
     * @param stats
     * @return
     */
    public static StorageType getHighestTier(DatanodeStatistics stats) {
        StorageType[] types = StorageType.valuesOrdered();
        for (int i = 0; i < types.length; i++) {
            if (stats.containsStorageTier(types[i])) {
                return types[i];
            }
        }
        
        return null;
    }

    /**
     * Get the lowest tiers from the existing tiers
     * @param stats
     * @return
     */
    public static StorageType getLowestTier(DatanodeStatistics stats) {
        StorageType[] types = StorageType.valuesOrdered();
        for (int i = types.length - 1; i >= 0; i--) {
            if (stats.containsStorageTier(types[i])) {
                return types[i];
            }
        }
        
        return null;
    }

    public static boolean isExistTier(StorageType type, DatanodeStatistics stats) {
        if (type != null)
            return stats.containsStorageTier(type);
        else 
            return false;
    }

    public static StorageType[] selectedDownTiers(DatanodeStatistics stats, StorageType type){
        ArrayList<StorageType> selectedDownTiersList = new ArrayList<StorageType>();
        boolean downTiers = false;
        StorageType[] types = StorageType.valuesOrdered();
        for (int i = 0; i < types.length; i++) {
            if (downTiers) {
                if (ReplicationManagementUtils.isExistTier(types[i], stats)) 
                    selectedDownTiersList.add(types[i]);   
            }
            else if (types[i].equals(type)) 
                downTiers = true;
        }

        return selectedDownTiersList.toArray(new StorageType[selectedDownTiersList.size()]);
    }

    public static StorageType[] selectedUpTiers(DatanodeStatistics stats, StorageType type){
        ArrayList<StorageType> selectedUpTiersList = new ArrayList<StorageType>();
        boolean upTiers = false;
        StorageType[] types = StorageType.valuesOrdered();
        for (int i = types.length-1; i>=0; i--) {
            if (upTiers) {
                if (ReplicationManagementUtils.isExistTier(types[i], stats)) 
                    selectedUpTiersList.add(types[i]);   
            }
            else if (types[i].equals(type)) 
                upTiers = true;
        }

        return selectedUpTiersList.toArray(new StorageType[selectedUpTiersList.size()]);
    }
    
   /**
    * Collect all files from the directory namespace and sort them based on
    * access time
    * 
    * @param dir
    * @return
    */
   public static ArrayList<INodeFile> collectSortedFiles(FSDirectory dir) {
      ArrayList<INodeFile> fileList = new ArrayList<INodeFile>();

      Iterator<INodeWithAdditionalFields> iter = dir.getINodeMap().getMapIterator();
      while (iter.hasNext()) {
         INodeWithAdditionalFields n = iter.next();
         if (n.isFile() && !n.asFile().isUnderConstruction())
            fileList.add(n.asFile());
      }

      Collections.sort(fileList, new Comparator<INodeFile>() {
         @Override
         public int compare(INodeFile o1, INodeFile o2) {
            return Long.compare(o1.getAccessTime(), o2.getAccessTime());
         }
      });

      return fileList;
   }
}
