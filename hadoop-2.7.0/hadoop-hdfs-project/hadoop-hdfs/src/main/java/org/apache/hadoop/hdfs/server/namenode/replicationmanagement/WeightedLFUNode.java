package org.apache.hadoop.hdfs.server.namenode.replicationmanagement;

import org.apache.hadoop.hdfs.server.namenode.INodeFile;

/**
 * LFU Node based on number of accesses
 * 
 * @author herodotos.herodotou
 *
 */
public class WeightedLFUNode extends WeightedNode {
   private long numAccesses;

   public WeightedLFUNode(INodeFile f) {
      super(f);
      this.numAccesses = 0;
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
   public String toString() {
      return "WeightedLFUNode [file=" + getFile() + ", access=" + numAccesses
            + "]";
   }

}
