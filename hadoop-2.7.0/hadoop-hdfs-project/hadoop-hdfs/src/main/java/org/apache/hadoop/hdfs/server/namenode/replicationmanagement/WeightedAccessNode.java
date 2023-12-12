package org.apache.hadoop.hdfs.server.namenode.replicationmanagement;

import static org.apache.hadoop.util.Time.now;

import org.apache.hadoop.hdfs.server.namenode.INodeFile;

/**
 * Weighted node based on both number and time of accesses
 * 
 * Weight = 1 + [bias / ((time_now - time_last_access) + bias)] * Weight
 * 
 * @author herodotos.herodotou
 */
public class WeightedAccessNode extends WeightedNode {

   // Represents "half life", i.e., after how much time the weight is halved
   public static double BIAS = 24d * 60 * 60 * 1000;

   private double weight;
   private long lastAccess;

   public WeightedAccessNode(INodeFile f) {
      super(f);
      this.lastAccess = f.getAccessTime();
      this.weight = BIAS / ((now() - this.lastAccess) + BIAS);
      if (this.weight > 0.99999d)
         this.weight = 1d; // this is a new file
   }

   @Override
   public int compareTo(WeightedNode o1) {
      if (this.file.equals(o1.file))
         return 0;

      WeightedAccessNode other = (WeightedAccessNode) o1;
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
      this.weight = 1 + BIAS * this.weight / ((now - this.lastAccess) + BIAS);
      this.lastAccess = now;
   }

   @Override
   double getWeight() {
      return weight;
   }

   @Override
   public String toString() {
      return "AWNode [file=" + getFile() + ", weight=" + weight + "]";
   }
}
