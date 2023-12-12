package org.apache.hadoop.hdfs.server.namenode.replicationmanagement;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;

/**
 * Represents a collection of file histories, which contain detailed information
 * about a file's size, creation time, and access times. The files in the
 * collection are ordered based on recency.
 * 
 * @author herodotos.herodotou
 */
public class FileHistoryCollection {

   // Enable the sharing of a FileHistoryCollection
   private static FileHistoryCollection fhCollection = null;
   private static int sharedCount = 0;

   public static FileHistoryCollection getInstance(int maxNumAccesses) {
      if (fhCollection == null)
         fhCollection = new FileHistoryCollection(maxNumAccesses);
      ++sharedCount;
      return fhCollection;
   }

   public static int getSharedCount() {
      return sharedCount;
   }

   // Private data members
   private AccessBasedList abList;
   private Map<Long, FileHistory> fileHistories;

   private int maxNumAccesses;

   /**
    * Private constructor
    */
   private FileHistoryCollection(int maxNumAccesses) {
      abList = new AccessBasedList();
      fileHistories = new HashMap<Long, FileHistory>();
      this.maxNumAccesses = maxNumAccesses;
   }

   /**
    * Add new file
    * 
    * @param file
    */
   public void addFile(INodeFile file) {
      if (containsFile(file))
         return;

      abList.addFile(file);
      fileHistories.put(file.getId(), new FileHistory(file.computeFileSize(),
            file.getModificationTime(), file.getAccessTime(), maxNumAccesses));
   }

   /**
    * Account for a file access
    * 
    * @param file
    */
   public void accessFile(INodeFile file) {
      if (file != null && fileHistories.containsKey(file.getId())) {
         abList.accessFile(file);
         FileHistory history = fileHistories.get(file.getId());
         history.addAccessTime(System.currentTimeMillis());

         if (file.getModificationTime() > history.getCreationTime()) {
            history.setFileSize(file.computeFileSize());
         }
      }
   }

   /**
    * Remove a file from the collection
    * 
    * @param file
    */
   public void removeFile(INodeFile file) {
      if (file != null) {
         abList.deleteFile(file);
         fileHistories.remove(file.getId());
      }
   }

   /**
    * Check if a file is in the collection
    * 
    * @param file
    * @return
    */
   public boolean containsFile(INodeFile file) {
      return file != null && fileHistories.containsKey(file.getId());
   }

   /**
    * Get file history associated with this file. Returns null if none is found.
    * 
    * @param file
    * @return
    */
   public FileHistory getFileHistory(INodeFile file) {
      if (file != null)
         return fileHistories.get(file.getId());
      else
         return null;
   }

   /**
    * @return An iterator to iterate the list from LRU to MRU file
    */
   public Iterator<INodeFile> getLRUFileIterator() {
      return abList.getLRUFileIterator();
   }

   /**
    * @return An iterator to iterate the list from MRU to LRU file
    */
   public Iterator<INodeFile> getMRUFileIterator() {
      return abList.getMRUFileIterator();
   }

   /**
    * @return The LRU file with a replica on the provided storage type
    */
   public INodeFile getLRUFile(StorageType type) {
      return abList.getLRUFile(type);
   }

   /**
    * A simple class to keep track of the access history of a file
    * 
    * @author herodotos.herodotou
    */
   static class FileHistory {
      private long fileSize;
      private long creationTime;
      private long[] accessTimes; // access times in chronological order
      private short accessCount; // num of accesses
      private short accessHead; // most recent access index

      FileHistory(long fileSize, long creationTime, long accessTime,
            int maxNumAccesses) {
         this.fileSize = fileSize;
         this.creationTime = creationTime;
         this.accessTimes = new long[maxNumAccesses];
         if (accessTime > creationTime) {
            this.accessTimes[0] = accessTime;
            this.accessHead = 0;
            this.accessCount = 1;
         } else {
            this.accessHead = -1;
            this.accessCount = 0;
         }
      }

      public void setFileSize(long fileSize) {
         this.fileSize = fileSize;
      }

      void addAccessTime(long accessTime) {
         accessHead = (short) ((accessHead + 1) % accessTimes.length);
         accessTimes[accessHead] = accessTime;
         if (accessCount < accessTimes.length)
            ++accessCount;
      }

      public long getFileSize() {
         return fileSize;
      }

      public long getCreationTime() {
         return creationTime;
      }

      public long getAccessTime(int i) {
         if (i < 0 || i >= accessCount)
            return 0;
         if (accessCount < accessTimes.length)
            return accessTimes[i];
         else
            return accessTimes[(accessHead + i + 1) % accessTimes.length];
      }

      public short getNumAccesses() {
         return accessCount;
      }
   }

}
