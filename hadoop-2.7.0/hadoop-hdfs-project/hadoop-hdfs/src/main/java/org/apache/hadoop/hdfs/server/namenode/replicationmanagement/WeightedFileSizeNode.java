package org.apache.hadoop.hdfs.server.namenode.replicationmanagement;

import org.apache.hadoop.hdfs.server.namenode.INodeFile;

/**
 * Weighted Node based on file size
 * 
 * @author elena.kakoulli
 *
 */

public class WeightedFileSizeNode extends WeightedNode {
   private long fileSize;

   public WeightedFileSizeNode(INodeFile f) {
      super(f);
      this.fileSize = f.computeFileSize();
   }

   @Override
   void updateWeight() {
	   //nothing to do
   }

   @Override
   double getWeight() {
      return fileSize;
   }

   @Override
   public String toString() {
      return "WeightedFileSizeNode [file=" + getFile() + ", file size=" + fileSize
            + "]";
   }

}
