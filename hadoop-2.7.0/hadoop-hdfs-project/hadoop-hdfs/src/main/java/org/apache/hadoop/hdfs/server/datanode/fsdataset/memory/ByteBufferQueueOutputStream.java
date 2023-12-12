package org.apache.hadoop.hdfs.server.datanode.fsdataset.memory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * An output stream for writing (or appending) data into a ByteBufferQueue.
 * 
 * Warning: The output stream is not thread safe. Only a single writer is
 * expected.
 */
public class ByteBufferQueueOutputStream extends OutputStream {

   // Data members
   private ByteBufferPool bbPool;
   private ByteBufferQueue bbQueue;
   private ByteBuffer currBuffer;
   private long writePos;

   private byte[] singleByte = new byte[1];

   /**
    * @param bbPool
    *           a ByteBufferPool for creating ByteBuffers
    * @param bbQueue
    *           the ByteBufferQueue to write into
    */
   public ByteBufferQueueOutputStream(ByteBufferPool bbPool,
         ByteBufferQueue bbQueue) {
      this.bbPool = bbPool;
      this.bbQueue = bbQueue;
      this.currBuffer = null;
      this.writePos = 0L;
   }

   @Override
   public void write(int b) throws IOException {
      singleByte[0] = (byte) b;
      write(singleByte, 0, 1);
   }

   @Override
   public void write(byte[] b) throws IOException {
      write(b, 0, b.length);
   }

   @Override
   public synchronized void write(byte[] b, int off, int len)
         throws IOException {
      // Basic error checking
      if (b == null) {
         throw new NullPointerException();
      } else if ((off < 0) || (off > b.length) || (len < 0)
            || ((off + len) > b.length) || ((off + len) < 0)) {
         throw new IndexOutOfBoundsException();
      } else if (len == 0) {
         return;
      }

      if (currBuffer == null) {
         // Get a new buffer
         currBuffer = bbPool.acquireByteBuffer();
      }

      int remaining = currBuffer.remaining();
      if (remaining == 0) {
         // The buffer is full, get a new one
         bbQueue.appendByteByffer(currBuffer);
         currBuffer = bbPool.acquireByteBuffer();
         remaining = currBuffer.capacity();
      }

      if (len <= remaining) {
         // There is enough space in the current buffer
         currBuffer.put(b, off, len);
         writePos += len;
      } else {
         // Put as much as you can in the current buffer
         // and recursively write the rest
         currBuffer.put(b, off, remaining);
         writePos += remaining;
         write(b, off + remaining, len - remaining);
      }

      if (currBuffer != null && currBuffer.remaining() == 0) {
         // The buffer is full but don't get a new one
         bbQueue.appendByteByffer(currBuffer);
         currBuffer = null;
      }
   }

   @Override
   public void flush() {
      // nothing to do
   }

   @Override
   public void close() {
      if (currBuffer != null) {
         if (currBuffer.position() > 0) {
            // Append the current buffer into the queue
            bbQueue.appendByteByffer(currBuffer);
         } else {
            // Release the buffer back to the pool
            bbPool.releaseByteBuffer(currBuffer);
         }

         currBuffer = null;
      }
   }

   /**
    * Explicitly set a new position for writing. The requested position must be
    * positive and less than the amount of data written so far.
    * 
    * @param position
    */
   public void setPosition(long position) {
      long size = bbQueue.size();
      if (position < size) {
         // Discard current buffer and rewind the queue
         if (currBuffer != null)
            bbPool.releaseByteBuffer(currBuffer);

         currBuffer = bbQueue.rewind(position, bbPool);

      } else if (position == size) {
         // Discard the current buffer
         if (currBuffer != null)
            currBuffer.clear();

      } else {
         if (currBuffer == null || position > size + currBuffer.position())
            throw new IndexOutOfBoundsException();

         // Rewind the current buffer
         currBuffer.position((int) (position - size));
      }
      
      writePos = position;
   }
   
   /**
    * @return the current write position
    */
   public long getPosition(){
	   return writePos;
   }
   
}
