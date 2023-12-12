package org.apache.hadoop.hdfs.server.datanode.fsdataset.memory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * An input stream for reading data from a ByteBufferQueue. The ByteBufferQueue
 * is allowed to grow and the input stream will read the new data one ByteBuffer
 * at a time.
 * 
 * Warning: This input stream is not thread safe. If you have multiple readers,
 * use multiple input streams.
 */
public class ByteBufferQueueInputStream extends InputStream {

   // Data members
   private ByteBufferQueue bbQueue;
   private ByteBuffer currBuffer;

   private Iterator<ByteBuffer> iterator;
   private long initialSize;
   private long totalPos;

   private boolean wait;

   private byte[] singleByte = new byte[1];
   
   /**
    * @param bbQueue
    *           The ByteBufferQueue to read from
    */
   public ByteBufferQueueInputStream(ByteBufferQueue bbQueue) {
      this(bbQueue, false);
   }
   
   /**
    * @param bbQueue The ByteBufferQueue to read from
    * @param wait If true, wait for new data to arrive when reading
    */
   public ByteBufferQueueInputStream(ByteBufferQueue bbQueue, boolean wait) {
      this.bbQueue = bbQueue;
      this.currBuffer = null;
      this.iterator = bbQueue.iterator();
      this.initialSize = bbQueue.size();
      this.totalPos = 0;
      this.wait = wait;
   }

   @Override
   public int read() throws IOException {
      if (read(singleByte, 0, 1) > 0)
         return singleByte[0];
      else
         return -1;
   }

   @Override
   public int read(byte[] b) {
      return read(b, 0, b.length);
   }

   @Override
   public int read(byte[] b, int off, int len) {
      // Basic error checking
      if (b == null) {
         throw new NullPointerException();
      } else if (off < 0 || len < 0 || len > b.length - off) {
         throw new IndexOutOfBoundsException();
      } else if (len == 0) {
         return 0;
      }

      if (!ensureCurrentBuffer())
         return -1;

      int remaining = currBuffer.remaining();

      if (len <= remaining) {
         // Read len bytes from the current buffer
         currBuffer.get(b, off, len);
         totalPos += len;
         return len;
      } else {
         // Read the current buffer and recursively read the rest
         currBuffer.get(b, off, remaining);
         totalPos += remaining;
         int rest = read(b, off + remaining, len - remaining);
         return (rest == -1) ? remaining : remaining + rest;
      }
   }

   @Override
   public void close() {
      // Move to the end of the queue
      do {
         currBuffer = null;
         wait = false;
      } while (ensureCurrentBuffer());

      totalPos = initialSize;
   }

   @Override
   public boolean markSupported() {
      return false;
   }

   @Override
   public long skip(long n) {
      if (n <= 0) {
         return 0;
      }

      if (!ensureCurrentBuffer())
         return 0;

      long toSkip = n;
      int remaining = currBuffer.remaining();
      while (toSkip >= remaining) {
         // Skip the remaining bytes from this buffer
         toSkip -= remaining;
         totalPos += remaining;

         currBuffer = null;
         if (!ensureCurrentBuffer())
            return n - toSkip;

         remaining = currBuffer.remaining();
      }

      if (toSkip > 0) {
         // Increase the buffers position to skip
         currBuffer.position(currBuffer.position() + (int) toSkip);
         totalPos += toSkip;
      }

      return n;
   }

   @Override
   public int available() {
      int available = (int) (bbQueue.size() - totalPos);
      return (available < 0) ? 0 : available;
   }

   /**
    * Ensure the current buffer is not null and available
    * 
    * @return true if successful
    */
   private boolean ensureCurrentBuffer() {

      if (initialSize != bbQueue.size()) {
         // The queue changed so start over
         reset(totalPos);
      }

      while (currBuffer == null || currBuffer.remaining() == 0) {

         if (initialSize != bbQueue.size()) {
            // The queue changed so start over
            reset(totalPos);
         } else if (iterator.hasNext()) {
            // Get next buffer, if any
            currBuffer = iterator.next().duplicate();
         } else {
            if (wait) {
               // Wait for new data to arrive. 
               // If this happens, bbQueue.size() will change
               try {
                  for (int i = 0; i < 5 && initialSize == bbQueue.size(); ++i) {
                     Thread.sleep(200);
                  }
               } catch (InterruptedException e) {
               }
            }
            
            if (initialSize == bbQueue.size()) {
               // No more buffers
               currBuffer = null;
               return false;
            }
         }
      }

      return true;
   }

   /**
    * Reset the iterator and skip to the current position
    * 
    * @param position
    */
   private void reset(long position) {
      iterator = bbQueue.iterator();
      initialSize = bbQueue.size();
      totalPos = 0;
      currBuffer = null;

      skip(position);
   }
}
