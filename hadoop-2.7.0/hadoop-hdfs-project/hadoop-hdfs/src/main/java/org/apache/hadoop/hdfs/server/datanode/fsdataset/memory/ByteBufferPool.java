package org.apache.hadoop.hdfs.server.datanode.fsdataset.memory;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A pool of reusable ByteBuffer objects
 */
public class ByteBufferPool {

   // Data members
   private ConcurrentLinkedQueue<ByteBuffer> bbPool;
   private int bbCapacity;

   /**
    * @param capacity
    *           capacity for each ByteBuffer in bytes
    */
   public ByteBufferPool(int capacity) {
      this.bbCapacity = capacity;
      this.bbPool = new ConcurrentLinkedQueue<ByteBuffer>();
   }

   /**
    * @return Either a new ByteBuffer or an existing ByteBuffer from the pool
    */
   public ByteBuffer acquireByteBuffer() {

      ByteBuffer buf = bbPool.poll();
      if (buf == null) {
         // Create new byte buffer
         buf = ByteBuffer.allocate(bbCapacity);
      }

      return buf;
   }

   /**
    * Return the ByteBuffer into the pool
    * 
    * @param bb
    */
   public void releaseByteBuffer(ByteBuffer bb) {
      if (bb != null) {
         bb.clear();
         bbPool.offer(bb);
      }
   }
   
   /**
    * Clear the pool
    */
   public void clear() {
      bbPool.clear();
   }
}
