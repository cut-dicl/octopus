package org.apache.hadoop.hdfs.server.namenode.replicationmanagement;

import static org.apache.hadoop.util.Time.now;

import org.apache.hadoop.hdfs.server.namenode.INodeFile;

/**
 * Weighted node based on a geometric relation between number and time of accesses
 * 
 * Weight = 1 + [e^(-alpha * (time_now - time_last_access)] * Weight
 * 
 * @author herodotos.herodotou
 */
public class WeightedEXDNode extends WeightedNode {

   // Exposes tradeoff between recency and frequency.
   // Larger values give more emphasis on recency
   public static double ALPHA = 1d / (24d * 60 * 60 * 1000);

   private double weight;
   private long lastAccess;

   public WeightedEXDNode(INodeFile f) {
      super(f);
      this.lastAccess = f.getAccessTime();
      this.weight = Math.exp(-1d * ALPHA * (now() - this.lastAccess));
      if (this.weight > 0.9999d)
         this.weight = 1d; // this is a new file
   }

   @Override
   public int compareTo(WeightedNode o1) {
      if (this.file.equals(o1.file))
         return 0;

      WeightedEXDNode other = (WeightedEXDNode) o1;
      int comp = Double.compare(this.weight, other.weight);
      if (comp == 0)
         comp = Long.compare(this.lastAccess, other.lastAccess);
      if (comp == 0)
         comp = (this.file.getId() < other.file.getId()) ? -1 : 1;

      return comp;
   }

   @Override
   void updateWeight() {
      long now = now();
      this.weight = 1
            + Math.exp(-1d * ALPHA * (now() - this.lastAccess)) * this.weight;
      this.lastAccess = now;
   }

   @Override
   double getWeight() {
      return weight;
   }

   @Override
   public String toString() {
      return "EXDNode [file=" + getFile() + ", weight=" + weight + "]";
   }
}
