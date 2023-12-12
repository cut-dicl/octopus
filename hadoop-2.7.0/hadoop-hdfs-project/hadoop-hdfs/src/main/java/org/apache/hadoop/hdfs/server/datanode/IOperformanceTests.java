package org.apache.hadoop.hdfs.server.datanode;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaOutputStreams;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.util.DataChecksum;

public class IOperformanceTests {

   public static final Log LOG = LogFactory.getLog(IOperformanceTests.class);

   private static String PERF_METRICS_FILENAME = "PERFMETRICS";
   private static String PROPERTY_WRITE_THROUGHPUT = "writeThroughput";
   private static String PROPERTY_READ_THROUGHPUT = "readThroughput";

   /**
    * Run the I/O perf test on the provided volume
    * 
    * @param volume
    * @param config
    * @param bpid
    * @throws IOException
    */
   public static void runIOperformanceTests(FsVolumeSpi volume,
         Configuration config, String bpid) throws IOException {
      long writeThroughput = 0;
      long readThroughput = 0;

      String filename = volume.getBasePath();
      File filePath = new File(filename, PERF_METRICS_FILENAME);

      if (!filePath.exists()) {
         // Need to perform the actual I/O tests
         long totalSize = config.getLongBytes(
               DFSConfigKeys.DFS_DATANODE_IO_TEST_BLOCK_SIZE_KEY,
               DFSConfigKeys.DFS_DATANODE_IO_TEST_BLOCK_SIZE_DEFAULT);
         int bufferSize = (int) config.getLongBytes(
               DFSConfigKeys.DFS_DATANODE_IO_TEST_BUFFER_SIZE_KEY,
               DFSConfigKeys.DFS_DATANODE_IO_TEST_BUFFER_SIZE_DEFAULT);

         ExtendedBlock block = new ExtendedBlock(bpid, 1, totalSize, 0);

         byte[] buffer = new byte[bufferSize];
         for (int i = 0; i < bufferSize; i++)
            buffer[i] = (byte) ('0' + i % 50);

         // Perform the tests
         writeThroughput = writeTest(volume, block, totalSize, bufferSize,
               buffer);

         readThroughput = readTest(volume, block, totalSize, bufferSize, buffer);

         // Delete the block
         Block invalidBlks[] = new Block[1];
         invalidBlks[0] = block.getLocalBlock();
         invalidBlks[0].setNumBytes(BlockCommand.NO_ACK);

         StorageType[] storages = {volume.getStorageType()};        
         volume.getDataset().invalidate(bpid, invalidBlks, storages); 

         // wait until the block is deleted
         try {
            File f = new File(new File(volume.getPath(bpid),
                  DataStorage.STORAGE_DIR_TMP), block.getBlockName());

            while (f.exists()) {
               try {
                  Thread.sleep(50);
               } catch (InterruptedException ignored) {
               }
            }
         } catch (IOException e) {
            if (!volume.getStorageType().equals(StorageType.MEMORY)) {
               // ignore if volume is memory
               throw new IOException("Problem with block deletion", e);
            }
         }

         // Save the write/read throughput values
         Properties props = new Properties();
         props.setProperty(PROPERTY_WRITE_THROUGHPUT,
               String.valueOf(writeThroughput));
         props.setProperty(PROPERTY_READ_THROUGHPUT,
               String.valueOf(readThroughput));

         try {
            Storage.writeProperties(filePath, null, props);
         } catch (IOException e) {
            throw new IOException("Failed to save the throughput values to "
                  + filePath.getAbsolutePath(), e);
         }

      } else {
         // Read the throughput values from the file
         try {
            Properties props = Storage.readPropertiesFile(filePath);

            writeThroughput = Long.parseLong(Storage.getProperty(
                  filePath.getParentFile(), props, PROPERTY_WRITE_THROUGHPUT));
            readThroughput = Long.parseLong(Storage.getProperty(
                  filePath.getParentFile(), props, PROPERTY_READ_THROUGHPUT));

         } catch (Exception e) {
            throw new IOException("Failed to read the throughput values from "
                  + filePath.getAbsolutePath(), e);
         }
      }

      // Check for any configured bandwidth values
      long writeBandwidth = config.getLong(
            DFSConfigKeys.DFS_DATANODE_VOLUME_WRITE_BANDWIDTHPERSEC_PREFIX_KEY
               + volume.getStorageType().toString(),
            DFSConfigKeys.DFS_DATANODE_VOLUME_BANDWIDTHPERSEC_DEFAULT);
      if (writeBandwidth > 0) {
         writeBandwidth = writeBandwidth / 1024;
         if (writeBandwidth < writeThroughput)
            writeThroughput = writeBandwidth;
      }

      long readBandwidth = config.getLong(
            DFSConfigKeys.DFS_DATANODE_VOLUME_READ_BANDWIDTHPERSEC_PREFIX_KEY
               + volume.getStorageType().toString(),
            DFSConfigKeys.DFS_DATANODE_VOLUME_BANDWIDTHPERSEC_DEFAULT);
      if (readBandwidth > 0) {
         readBandwidth = readBandwidth / 1024;
         if (readBandwidth < readThroughput)
            readThroughput = readBandwidth;
      }
      
      // Set the throughput values
      volume.setWriteThroughput(writeThroughput);
      volume.setReadThroughput(readThroughput);

      LOG.info("[" + volume.getStorageType() + "]" + volume.getBasePath()
            + ": Write Throughput (kb/s) = " + String.valueOf(writeThroughput)
            + " -- Read Throughput (kb/s) = " + String.valueOf(readThroughput));
   }

   /**
    * Run the read test on the provided volume
    * 
    * @param volume
    * @param block
    * @param totalSize
    * @param bufferSize
    * @param buffer
    * @return
    * @throws IOException
    */
   private static long readTest(FsVolumeSpi volume, ExtendedBlock block,
         long totalSize, int bufferSize, byte[] buffer) throws IOException {

      long tStart = System.currentTimeMillis();
      
      InputStream in = null;

      long actualSize = 0;
      try {
         in = volume.getDataset().getBlockInputStream(block, 0);

         while (actualSize < totalSize) {
            int curSize = in.read(buffer, 0, bufferSize);
            if (curSize < 0) {
               break;
            } else if (curSize < bufferSize) {
               // Perform reads until the current buffer is full
               int toRead = bufferSize - curSize;
               while (toRead > 0) {
                 int ret = in.read(buffer, curSize, toRead);
                 if (ret < 0) break;
                 toRead -= ret;
                 curSize += ret;
               }
            }
            actualSize += curSize;
         }

         // Signal the OS to avoid caching
         if (in instanceof FileInputStream) {
            FileDescriptor inFD = ((FileInputStream) in).getFD();

            NativeIO.POSIX.getCacheManipulator().posixFadviseIfPossible(
                  block.getBlockName(), inFD, 0, actualSize,
                  NativeIO.POSIX.POSIX_FADV_DONTNEED);
         }

      } catch (IOException e) {
         throw new IOException("Problem reading the block " + block, e);
      } finally {
         if (in != null)
            in.close();
      }

      long execTime = System.currentTimeMillis() - tStart;
      if (execTime == 0L) {
         LOG.warn("The I/O write test ended in 0ms. Try increasing the test size.");
         execTime = 1L;
      }

      // return throughput in kb/s
      return (long) (actualSize * 1000.0 / (execTime * 0x400L));
   }

   /**
    * Run the write test on the provided volume
    * 
    * @param volume
    * @param block
    * @param totalSize
    * @param bufferSize
    * @param buffer
    * @return
    * @throws IOException
    */
   private static long writeTest(FsVolumeSpi volume, ExtendedBlock block,
         long totalSize, int bufferSize, byte[] buffer) throws IOException {

      long tStart = System.currentTimeMillis();

      ReplicaInPipelineInterface replica = volume
            .getDataset()
            .createTemporary(volume.getStorageID(), volume.getStorageType(),
                  block).getReplica();
      ReplicaOutputStreams streams = null;

      try {
         // Create the file stream and write the data
         streams = replica.createStreams(true,
               DataChecksum.newDataChecksum(DataChecksum.Type.CRC32, 512));

         OutputStream dataOut = streams.getDataOut();

         for (long nrRemaining = totalSize; nrRemaining > 0; nrRemaining -= bufferSize) {
            long curSize = (bufferSize < nrRemaining) ? bufferSize
                  : nrRemaining;

            dataOut.write(buffer, 0, (int) curSize);
            dataOut.flush();
         }

         // Try to flush to the storage media and avoid caching
         if (dataOut instanceof FileOutputStream) {
            FileDescriptor outFD = ((FileOutputStream) dataOut).getFD();
            FileChannel outFC = ((FileOutputStream) dataOut).getChannel();

            outFC.force(true);
            outFD.sync();

            NativeIO.POSIX.getCacheManipulator().posixFadviseIfPossible(
                  block.getBlockName(), outFD, 0, totalSize,
                  NativeIO.POSIX.POSIX_FADV_DONTNEED);
         }

      } catch (IOException e) {
         throw new IOException("Unable to write in block " + block, e);
      } finally {
         if (streams != null)
            streams.close();
      }

      long execTime = System.currentTimeMillis() - tStart;
      if (execTime == 0L) {
         LOG.warn("The I/O write test ended in 0ms. Try increasing the test size.");
         execTime = 1L;
      }

      // return throughput in kb/s
      return (long) (totalSize * 1000.0 / (execTime * 0x400L));
   }

}
