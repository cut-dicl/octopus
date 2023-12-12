package org.apache.hadoop.hdfs.server.namenode.replicationmanagement;

import java.util.Iterator;

import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.replicationmanagement.AccessBasedList.Node;

/**
 * This collection is inspired by the database marketing analysis called
 * "Recency, Frequency, Monetary" (RFM). The idea behind RFM is simple: 1)
 * Customers with recent purchases are more likely to buy again; 2) Customers
 * with frequent purchases are more likely to buy again; 3) Customers who spend
 * more are more likely to buy again. RFM groups customers into segments based
 * on their purchase activity. These segments are ordered from most valuable to
 * least valuable.
 * 
 * In our case, the files are the customers, purchases are file accesses,
 * recency has the same meaning, frequency has the same meaning, and monetary
 * can be the file size.
 * 
 * In practice, all files are split into 3 segments based on recency. The files
 * within each segment are then sorted based on frequency and file size.
 * 
 * @author herodotos.herodotou
 */
public class RFMCollection {

   // Enable the sharing of a collection
   private static RFMCollection sharedRFM = null;
   private static int sharedCount = 0;

   public static RFMCollection getSharedRFM() {
      if (sharedRFM == null)
         sharedRFM = new RFMCollection();
      ++sharedCount;
      return sharedRFM;
   }
   
   public static int getSharedCount() {
      return sharedCount;
   }
   
   // Privatre data members
   private AccessBasedList[] recency;
   private SortedWeightedTree<FMNode>[] frequency;
   private int[] counts;
   private int totalCount;

   @SuppressWarnings("unchecked")
   public RFMCollection() {
      recency = new AccessBasedList[3];
      frequency = new SortedWeightedTree[3];
      counts = new int[3];
      totalCount = 0;

      for (int i = 0; i < 3; ++i) {
         recency[i] = new AccessBasedList();
         frequency[i] = new SortedWeightedTree<FMNode>();
         counts[i] = 0;
      }
   }

   /**
    * Add new file to the collection
    * 
    * @param file
    */
   public void addFile(INodeFile file) {
      if (containsFile(file))
         return;

      // Add file to front list/tree
      recency[2].addFile(file);
      frequency[2].addNode(new FMNode(file));
      ++totalCount;

      // Rebalance everything
      recomputeCounts();
      rebalanceOnAdd();
   }

   /**
    * Account for a file access
    * 
    * @param file
    */
   public void accessFile(INodeFile file) {

      int index = -1;
      if (recency[0].containsFile(file))
         index = 0;
      else if (recency[1].containsFile(file))
         index = 1;
      else if (recency[2].containsFile(file))
         index = 2;

      if (index == 0 || index == 1) {
         // Move to the front list/tree
         recency[2].addFileNode(recency[index].deleteFile(file));
         WeightedNode wNode = frequency[index].deleteNode(file);
         wNode.updateWeight();
         frequency[2].addNode(wNode);

         rebalanceOnAdd();
      } else if (index == 2) {
         // Already in the front list/tree
         recency[2].accessFile(file);
         frequency[2].updateNode(file);
      }
   }

   /**
    * Remove a file from the collection
    * 
    * @param file
    */
   public void removeFile(INodeFile file) {
      boolean deleted = false;
      for (int i = 0; i < 3 && !deleted; ++i) {
         if (recency[i].containsFile(file)) {
            // Delete the file
            recency[i].deleteFile(file);
            frequency[i].deleteNode(file);
            --totalCount;

            // Rebalance everything
            recomputeCounts();
            rebalanceOnDelete();

            deleted = true;
         }
      }
   }

   /**
    * Check if a file is in the collection
    * 
    * @param file
    * @return
    */
   public boolean containsFile(INodeFile file) {
      for (int i = 0; i < 3; ++i) {
         if (recency[i].containsFile(file))
            return true;
      }

      return false;
   }

   /**
    * @return ascending iterator from LRU/LFU to MRU/MFU
    */
   public Iterator<INodeFile> ascFileIter() {
      return new RFMFileIterator(frequency, true);
   }

   /**
    * @return descending iterator from MRU/MFU to LRU/LFU
    */
   public Iterator<INodeFile> descFileIter() {
      return new RFMFileIterator(frequency, false);
   }

   public int size() {
      return totalCount;
   }

   private void recomputeCounts() {
      counts[0] = totalCount / 3;
      counts[1] = (totalCount + 1) / 3;
      counts[2] = (totalCount + 2) / 3;
   }

   private void rebalanceOnAdd() {
      for (int i = 2; i >= 1; --i) {
         if (recency[i].size() > counts[i]) {
            // Move LRU to previous list
            Node lNode = recency[i].removeLRUFileNode();
            WeightedNode wNode = frequency[i].deleteNode(lNode.getFile());
            recency[i - 1].addFileNode(lNode);
            frequency[i - 1].addNode(wNode);
         }
      }
   }

   private void rebalanceOnDelete() {
      for (int i = 2; i >= 1; --i) {
         if (recency[i].size() < counts[i]) {
            // Move MRU from previous list
            Node mNode = recency[i - 1].removeMRUFileNode();
            WeightedNode wNode = frequency[i - 1].deleteNode(mNode.getFile());
            recency[i].addFileNodeLast(mNode);
            frequency[i].addNode(wNode);
         } else if (recency[i].size() > counts[i]) {
            // Move LRU to previous list
            Node lNode = recency[i].removeLRUFileNode();
            WeightedNode wNode = frequency[i].deleteNode(lNode.getFile());
            recency[i - 1].addFileNode(lNode);
            frequency[i - 1].addNode(wNode);
         }
      }
   }

   private class RFMFileIterator implements Iterator<INodeFile> {

      SortedWeightedTree<FMNode>[] freq;
      boolean asc;
      int index;
      Iterator<INodeFile> currIter;

      public RFMFileIterator(SortedWeightedTree<FMNode>[] freq, boolean asc) {
         this.freq = freq;
         this.asc = asc;
         this.index = asc ? 0 : 2;
         this.currIter = asc ? freq[index].ascFileIter() : freq[index].descFileIter();
      }

      @Override
      public boolean hasNext() {
         if (currIter.hasNext())
            return true;

         while ((asc && index < 2) || (!asc && index > 0)) {
            index = asc ? ++index : --index;
            currIter = asc ? freq[index].ascFileIter() : freq[index].descFileIter();
            if (currIter.hasNext())
               return true;
         }

         return false;
      }

      @Override
      public INodeFile next() {
         return currIter.next();
      }
   }

   /**
    * Node based on frequency and file size
    */
   private class FMNode extends WeightedNode {
      private long numAccesses;
      private long fileSize;

      public FMNode(INodeFile f) {
         super(f);
         this.numAccesses = 0;
         this.fileSize = f.computeFileSize();
      }

      @Override
      void updateWeight() {
         this.numAccesses = this.numAccesses + 1;
      }

      @Override
      double getWeight() {
         return numAccesses;
      }

      @Override
      public int compareTo(WeightedNode o1) {
         if (this.file.equals(o1.file))
            return 0;

         FMNode other = (FMNode) o1;
         int comp = Double.compare(this.numAccesses, other.numAccesses);
         if (comp == 0)
            comp = Long.compare(other.fileSize, this.fileSize);
         if (comp == 0)
            comp = (this.file.getId() < other.file.getId()) ? -1 : 1;

         return comp;
      }

      @Override
      public String toString() {
         return "FMNode [file=" + getFile() + ", access=" + numAccesses
               + ", size=" + fileSize + "]";
      }
   }
}
