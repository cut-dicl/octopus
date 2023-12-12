/**
 * 
 */
package org.apache.hadoop.mapreduce.prefetch;

import java.util.Comparator;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.StorageType;

/**
 * This class models the execution of a task based on reading its input data.
 * 
 * @author hero
 */
public class MRTaskModel {

   private FileStatus fileStatus;
   private long length;

   private String[] hosts;
   private String[] storageIDs;
   private StorageType[] types;

   private StorageType baseStorageType; // Storage type of base replica

   private boolean isAlreadyCached;

   private double startTime; // Expected start time
   private double endTime; // Expected end time
   private double cacheTime; // Expected time a cache replica will be available

   private int candCacheIndex; // Index of source replica for caching

   /**
    * Constructor
    * 
    * @param fileStatus
    * @param length
    * @param hosts
    * @param types
    * @param baseStorageType
    * @param isAlreadyCached
    */
   public MRTaskModel(FileStatus fileStatus, long length, String[] hosts, String[] storageIDs,
         StorageType[] types, StorageType baseStorageType, boolean isAlreadyCached) {
      this.fileStatus = fileStatus;
      this.length = length;
      this.hosts = hosts;
      this.storageIDs = storageIDs;
      this.types = types;
      this.baseStorageType = baseStorageType;
      this.isAlreadyCached = isAlreadyCached;

      this.startTime = 0;
      this.endTime = 0;
      this.cacheTime = -1;

      this.candCacheIndex = -1;
   }

   public FileStatus getFileStatus() {
      return fileStatus;
   }

   public long getLength() {
      return length;
   }

   public String[] getHosts() {
      return hosts;
   }

   public String[] getStorageIDs() {
      return storageIDs;
   }

   public StorageType[] getStorageTypes() {
      return types;
   }

   public StorageType getBaseStorageType() {
      return baseStorageType;
   }

   public boolean isAlreadyCached() {
      return isAlreadyCached;
   }

   public double getStartTime() {
      return startTime;
   }

   public double getEndTime() {
      return endTime;
   }

   public double getCacheTime() {
      return cacheTime;
   }

   public boolean hasCandidateCacheLoc() {
      return candCacheIndex >= 0;
   }

   public String getCandidateCacheHost() {
      return candCacheIndex >= 0 ? hosts[candCacheIndex] : null;
   }

   public String getCandidateCacheStorageID() {
      return candCacheIndex >= 0 ? storageIDs[candCacheIndex] : null;
   }

   public StorageType getCandidateCacheType() {
      return candCacheIndex >= 0 ? types[candCacheIndex] : null;
   }

   public boolean isReadFullyFromCache() {
      return isAlreadyCached || (cacheTime > 0 && startTime >= cacheTime);
   }

   public boolean isReadPartiallyFromCache() {
      return MRExecutionModel.EnablePartialCacheRead && !isAlreadyCached
            && (cacheTime > 0 && startTime < cacheTime);
   }

   public void setStartTime(double startTime) {
      this.startTime = startTime;
   }

   public void setEndTime(double endTime) {
      this.endTime = endTime;
   }

   public void setCacheTime(double cacheTime) {
      this.cacheTime = cacheTime;
   }

   public void setCandidateCacheLocation(int candidateCacheIndex) {
      this.candCacheIndex = candidateCacheIndex;
   }

   public void resetCaching() {
      this.cacheTime = -1;
      this.candCacheIndex = -1;
   }

   @Override
   public String toString() {
      String readFrom;
      if (isReadFullyFromCache())
         readFrom = "CACHE";
      else if (isReadPartiallyFromCache())
         readFrom = "CACHE+" + baseStorageType.toString();
      else
         readFrom = baseStorageType.toString();

      return String.format("MRTaskModel: p=%s l=%d s=%.2f c=%.2f f=%.2f t=%s",
            fileStatus.getPath().getName(), length, startTime / 1000, cacheTime / 1000,
            endTime / 1000, readFrom);
   }

   /**
    * A comparator for sorting task models by length decreasing
    * 
    * @author hero
    */
   static class MRTaskComparatorByLengthDecr implements Comparator<MRTaskModel> {

      @Override
      public int compare(MRTaskModel m1, MRTaskModel m2) {
         if (m1.length < m2.length)
            return 1;
         else if (m1.length > m2.length)
            return -1;
         else
            return 0;
      }

   }

   /**
    * A comparator for sorting task models by length increasing
    * 
    * @author hero
    */
   static class MRTaskComparatorByLengthIncr implements Comparator<MRTaskModel> {

      @Override
      public int compare(MRTaskModel m1, MRTaskModel m2) {
         if (m1.length > m2.length)
            return 1;
         else if (m1.length < m2.length)
            return -1;
         else
            return 0;
      }

   }

}
