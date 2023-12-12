package org.apache.hadoop.hdfs.server.datanode.fsdataset.memory;

import java.util.concurrent.atomic.AtomicLong;

public class MemoryStats {

   private AtomicLong reportedUsage; // bytes used for data
   private AtomicLong actualUsage; // total bytes used based on BB capacity
   private MemoryStats parentStats; // stats for parent object, maybe null

   public MemoryStats() {
      this.reportedUsage = new AtomicLong(0L);
      this.actualUsage = new AtomicLong(0L);
      this.parentStats = null;
   }

   public MemoryStats(MemoryStats parentStats) {
      this.reportedUsage = new AtomicLong(0L);
      this.actualUsage = new AtomicLong(0L);
      this.parentStats = parentStats;
   }

   public long getReportedUsage() {
      return this.reportedUsage.get();
   }

   public long getActualUsage() {
      return this.actualUsage.get();
   }

   public void decrement(long reported, long actual) {
      long oldValue, newValue;
      if (reported != 0) {
         do {
            oldValue = reportedUsage.get();
            newValue = oldValue - reported;
            if (newValue < 0)
               newValue = 0;
         } while (!reportedUsage.compareAndSet(oldValue, newValue));
      }

      if (actual != 0) {
         do {
            oldValue = actualUsage.get();
            newValue = oldValue - actual;
            if (newValue < 0)
               newValue = 0;
         } while (!actualUsage.compareAndSet(oldValue, newValue));
      }

      if (this.parentStats != null)
         this.parentStats.decrement(reported, actual);
   }

   public void increment(long reported, long actual) {
      this.reportedUsage.addAndGet(reported);
      this.actualUsage.addAndGet(actual);
      if (this.parentStats != null)
         this.parentStats.increment(reported, actual);
   }

   @Override
   public String toString() {
      return "MemoryStats [reported=" + reportedUsage + ", actual="
            + actualUsage + ", parent="
            + ((parentStats == null) ? "null" : parentStats) + "]";
   }

}
