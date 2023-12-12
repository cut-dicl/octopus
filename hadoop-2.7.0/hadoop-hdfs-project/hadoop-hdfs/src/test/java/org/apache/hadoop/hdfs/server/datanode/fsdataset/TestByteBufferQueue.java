package org.apache.hadoop.hdfs.server.datanode.fsdataset;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.hdfs.server.datanode.fsdataset.memory.ByteBufferPool;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.memory.ByteBufferQueue;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.memory.ByteBufferQueueInputStream;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.memory.ByteBufferQueueOutputStream;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.memory.MemoryStats;

public class TestByteBufferQueue {

   public static void main(String[] args) throws IOException, InterruptedException {

      final ByteBufferPool pool = new ByteBufferPool(20);
      final ByteBufferQueue bbq = new ByteBufferQueue(new MemoryStats());

      String s1 = "Optional: Configure DiffMerge on Windows. Note: In case of conflicts, fix them and do 'git commit'";
      final byte[] b1 = s1.getBytes();
      int n1 = b1.length;

      // write
      OutputStream out = new ByteBufferQueueOutputStream(pool, bbq);
      out.write(b1[0]);
      out.write(b1[1]);
      out.write(b1, 2, 28);

      // read
      byte[] b2 = new byte[b1.length + 8];
      InputStream in2 = new ByteBufferQueueInputStream(bbq);
      b2[0] = (byte) in2.read();
      int n2 = 1;
      n2 += in2.read(b2, 1, 15);

      byte[] b3 = new byte[b1.length + 8];
      InputStream in3 = new ByteBufferQueueInputStream(bbq);
      int n3 = 0;

      n3 += in3.read(b3, 0, 20);

      out.write(b1, 30, b1.length - 30);

      n2 += in2.read(b2, 16, 30);
      n3 += in3.read(b3, 20, 40);

      out.close();

      n2 += in2.read(b2, 46, b2.length - 46);
      in2.close();

      n3 += in3.read(b3, 60, b3.length - 60);
      in3.close();

      byte[] b4 = new byte[b1.length];
      InputStream in4 = new ByteBufferQueueInputStream(bbq);
      int n4 = 0;

      n4 = in4.read(b4, 0, 8);
      in4.skip(35);
      n4 = in4.read(b4);

      in4.close();

      // Append
      OutputStream out5 = new ByteBufferQueueOutputStream(pool, bbq);
      out5.write("Appending string".getBytes());
      out5.close();

      byte[] b6 = new byte[b1.length + 40];
      InputStream in6 = new ByteBufferQueueInputStream(bbq);
      int n6 = in6.read(b6);

      in6.close();

      ByteBufferQueueOutputStream out7 = new ByteBufferQueueOutputStream(pool, bbq);
      byte[] b8 = new byte[b1.length + 40];
      ByteBufferQueueInputStream in8 = new ByteBufferQueueInputStream(bbq);

      out7.setPosition(92);
      out7.write("Overwrite".getBytes());
      out7.close();

      int n8 = in8.read(b8);

      in8.close();

      ByteBufferQueue bbq2 = bbq.copy();
      bbq2.delete(pool);

      String s2 = new String(b2);
      String s3 = new String(b3);
      String s4 = new String(b4);
      String s6 = new String(b6);
      String s8 = new String(b8);

      System.out.println("Size: " + n1);
      System.out.println("Str: '" + s1 + "'");
      System.out.println("Size: " + n2);
      System.out.println("Str: '" + s2 + "'");
      System.out.println("Size: " + n3);
      System.out.println("Str: '" + s3 + "'");
      System.out.println("Size: " + n4);
      System.out.println("Str: '" + s4 + "'");
      System.out.println("Size: " + n6);
      System.out.println("Str: '" + s6 + "'");
      System.out.println("Size: " + n8);
      System.out.println("Str: '" + s8 + "'");

      // Create a thread for writing data slowly
      bbq.delete(pool);
      Thread writer = new Thread(new Runnable() {

         @Override
         public void run() {
            OutputStream out10 = new ByteBufferQueueOutputStream(pool, bbq);
            int offset = 0, remain = 0;

            while (offset < b1.length) {
               try {
                  Thread.sleep(100);
               } catch (InterruptedException e) {
                  e.printStackTrace();
               }

               try {
                  remain = Math.min(15, b1.length - offset);
                  System.out.println("Writing " + remain);
                  out10.write(b1, offset, remain);
               } catch (IOException e) {
                  e.printStackTrace();
               }

               offset += remain;
            }

            try {
               out10.close();
            } catch (IOException e) {
               e.printStackTrace();
            }
         }
      });

      writer.start();

      // Read the data while they are written
      ByteBufferQueueInputStream in10 = new ByteBufferQueueInputStream(bbq, true);
      byte[] b10 = new byte[b1.length];
      int n10 = in10.read(b10);
      in10.close();

      writer.join();

      String s10 = new String(b10);

      System.out.println("Size: " + n10);
      System.out.println("Str: '" + s10 + "'");
   }

}
