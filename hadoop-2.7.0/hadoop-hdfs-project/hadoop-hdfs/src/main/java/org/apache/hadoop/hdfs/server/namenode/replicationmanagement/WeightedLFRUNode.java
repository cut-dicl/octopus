package org.apache.hadoop.hdfs.server.namenode.replicationmanagement;

import java.util.Comparator;

import org.apache.hadoop.hdfs.server.namenode.INodeFile;

/**
 * Weighted Node that keeps track both of frequency and recency data. To be used
 * with comparators for getting either LFU or LRU functionality. Will default to
 * LRU by itself.
 * 
 * @author herodotos.herodotou
 */
public class WeightedLFRUNode extends WeightedNode {
   protected long numAccesses;
   protected long lastAccessTime;

   public WeightedLFRUNode(INodeFile f) {
      super(f);
      this.numAccesses = 0;
      this.lastAccessTime = f.getAccessTime();
   }

   @Override
   void updateWeight() {
      this.numAccesses = this.numAccesses + 1;
      this.lastAccessTime = System.currentTimeMillis();
   }

   @Override
   double getWeight() {
      return lastAccessTime; // Defaults to LRU
   }

   public long getLastAccessTime() {
      return lastAccessTime;
   }
   
   @Override
   public String toString() {
      return "WeightedLFRUNode [file=" + getFile() + ", numAccesses="
            + numAccesses + ", lastAccess=" + lastAccessTime + "]";
   }

   /**
    * Custom LFU comparator
    * 
    * @author herodotos.herodotou
    */
   static class LFUComparator implements Comparator<WeightedNode> {
      @Override
      public int compare(WeightedNode o1, WeightedNode o2) {
         return compare((WeightedLFRUNode) o1, (WeightedLFRUNode) o2);
      }

      private int compare(WeightedLFRUNode o1, WeightedLFRUNode o2) {
         if (o1.file.equals(o2.file))
            return 0;

         int comp = Double.compare(o1.numAccesses, o2.numAccesses);
         if (comp == 0)
            return (o1.file.getId() < o2.file.getId()) ? -1 : 1;
         else
            return comp;
      }
   }

   /**
    * Custom LRU comparator
    * 
    * @author herodotos.herodotou
    */
   static class LRUComparator implements Comparator<WeightedNode> {
      @Override
      public int compare(WeightedNode o1, WeightedNode o2) {
         return compare((WeightedLFRUNode) o1, (WeightedLFRUNode) o2);
      }

      private int compare(WeightedLFRUNode o1, WeightedLFRUNode o2) {
         if (o1.file.equals(o2.file))
            return 0;

         int comp = Double.compare(o1.lastAccessTime, o2.lastAccessTime);
         if (comp == 0)
            return (o1.file.getId() < o2.file.getId()) ? -1 : 1;
         else
            return comp;
      }
   }
}
