/**
 * 
 */
package org.apache.hadoop.mapreduce.prefetch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ReplicationVector;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage.State;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * A simple class for testing the MRExecutionModel
 * 
 * @author hero
 */
public class TestMRExecutionModel {

   private static long MB = 1024l * 1024l;

   /**
    * @param args
    * @throws InterruptedException
    * @throws IOException
    */
   public static void main(String[] args) throws IOException, InterruptedException {

      Configuration conf = new Configuration();
      conf.setBoolean("dfs.datanode.enable.partial.cache.read", false);

      int numFilesDomain[] = {2, 4, 8, 16, 32, 64, 128};
      int numNodesDomain[] = {4, 8, 16, 32, 64, 128, 256, 512, 1024};
      int numDisksDomain[] = {3, 6, 12};

      for (int i = 0; i < numFilesDomain.length; ++i) {
         for (int j = 0; j < numNodesDomain.length; ++j) {
            for (int k = 0; k < numDisksDomain.length; ++k) {
               runExperiment(conf, numFilesDomain[i], numNodesDomain[j], numDisksDomain[k]);
            }
         }
      }
   }
   
   private static void runExperiment(Configuration conf, int numFiles, int numNodes, int numDisksPerNode) throws IOException, InterruptedException {

      Random rnd = new Random(42);
      int numCoresPerNode = 8;
      int memSizePerNode = 4 * 1024;
      int diskSize = 640 * 1024;

      int numFiles1 = numFiles;
      int numBlocksPerFile1 = 8;
      int lastBlockMB1 = 16;

      int numFiles2 = 0;
      int numBlocksPerFile2 = 3;
      int lastBlockMB2 = 9;

      ClusterMetrics metrics = new ClusterMetrics(0, 0, 0, 0, 0, 0, 0, 0, 0, numNodes, 0, 0, 0,
            memSizePerNode * numNodes, memSizePerNode * numNodes, 0, numCoresPerNode * numNodes,
            numCoresPerNode * numNodes, 0, 0, 0);

      Collection<FileStatus> files = generateFiles(rnd, "/user/test1", numFiles1, numBlocksPerFile1, numNodes,
            numDisksPerNode, lastBlockMB1);
      files.addAll(generateFiles(rnd, "/user/test2", numFiles2, numBlocksPerFile2, numNodes,
            numDisksPerNode, lastBlockMB2));
      DatanodeStorageReport[] report = generateStorageReports(rnd, numNodes, numDisksPerNode, memSizePerNode, diskSize);

      long maxWait = -1;
      long maxSize = memSizePerNode * numNodes * MB;

      MRPrefetcher.isPrefetchingEnabled(conf);
      Logger.getLogger(MRPrefetcher.class).setLevel(Level.INFO);
      
      long startTime = System.currentTimeMillis();
      MRPrefetcher.prefetchSelectiveFiles(files, metrics, report, conf, null, maxWait, maxSize);
      long endTime = System.currentTimeMillis();
      System.out.println(String.format(
            "Evaluation: files=%d blocks=%d nodes=%d slots=%d disks=%d duration=%d",
            numFiles1 + numFiles2, numFiles1 * numBlocksPerFile1 + numFiles2 * numBlocksPerFile2,
            numNodes, numNodes * numCoresPerNode, numNodes * numDisksPerNode, endTime - startTime));
   }

   /**
    * @return a synthetic set of files with block locations
    */
   static Collection<FileStatus> generateFiles() {

      ArrayList<FileStatus> files = new ArrayList<FileStatus>();

      // File 1
      BlockLocation[] blks1 = new BlockLocation[2];
      blks1[0] = new BlockLocation(null, new String[] { "host1", "host2", "host3" },
            new StorageType[] { StorageType.MEMORY, StorageType.DISK, StorageType.DISK }, null,
            null, 0, 128 * MB, false, new String[] { "sr1m1", "sr2d1", "sr3d1" }, null);
      blks1[1] = new BlockLocation(null, new String[] { "host1", "host3", "host4" },
            new StorageType[] { StorageType.DISK, StorageType.MEMORY, StorageType.DISK }, null,
            null, 128 * MB, 72 * MB, false, new String[] { "sr1d1", "sr3m1", "sr4d2" }, null);
      LocatedFileStatus lfs1 = new LocatedFileStatus(200 * MB, false,
            ReplicationVector.ParseReplicationVector("M=1,D=2"), 128 * MB, 0, 0, null, "test",
            "test", null, new Path("/user/test/f1"), blks1);
      files.add(lfs1);

      // File 2
      BlockLocation[] blks2 = new BlockLocation[3];
      blks2[0] = new BlockLocation(null, new String[] { "host2", "host4", "host5" },
            new StorageType[] { StorageType.SSD, StorageType.DISK, StorageType.DISK }, null, null,
            0, 128 * MB, false, new String[] { "sr2s1", "sr4d1", "sr5d1" }, null);
      blks2[1] = new BlockLocation(null, new String[] { "host1", "host4", "host5" },
            new StorageType[] { StorageType.SSD, StorageType.DISK, StorageType.DISK }, null, null,
            128 * MB, 118 * MB, false, new String[] { "sr1s1", "sr4d1", "sr5d2" }, null);
      blks2[2] = new BlockLocation(null, new String[] { "host1", "host3", "host4" },
            new StorageType[] { StorageType.DISK, StorageType.DISK, StorageType.SSD }, null, null,
            246 * MB, 44 * MB, false, new String[] { "sr1d2", "sr3d2", "sr4s1" }, null);
      LocatedFileStatus lfs2 = new LocatedFileStatus(300 * MB, false,
            ReplicationVector.ParseReplicationVector("S=1,D=2"), 128 * MB, 0, 0, null, "test",
            "test", null, new Path("/user/test/f2"), blks2);
      files.add(lfs2);

      // File 3
      BlockLocation[] blks3 = new BlockLocation[1];
      blks3[0] = new BlockLocation(null, new String[] { "host1", "host3", "host5" },
            new StorageType[] { StorageType.DISK, StorageType.DISK, StorageType.DISK },
            new String[] { "host3" }, null, 0, 120 * MB, false,
            new String[] { "sr1d1", "sr3d1", "sr5d1" }, null);
      LocatedFileStatus lfs3 = new LocatedFileStatus(120 * MB, false,
            ReplicationVector.ParseReplicationVector("D=3"), 120 * MB, 0, 0, null, "test", "test",
            null, new Path("/user/test/f3"), blks3);
      files.add(lfs3);

      return files;
   }

   /**
    * @return a synthetic set of datanode information
    */
   static DatanodeStorageReport[] generateStorageReports() {

      DatanodeStorageReport dnsr1 = generateStorageReport(1, 200, 400);
      DatanodeStorageReport dnsr2 = generateStorageReport(2, 250, 350);
      DatanodeStorageReport dnsr3 = generateStorageReport(3, 180, 420);
      DatanodeStorageReport dnsr4 = generateStorageReport(4, 100, 500);
      DatanodeStorageReport dnsr5 = generateStorageReport(5, 450, 150);

      return new DatanodeStorageReport[] { dnsr1, dnsr2, dnsr3, dnsr4, dnsr5 };
   }

   /**
    * @return a synthetic datanode information with 1 mem, 1 ssd, and 2 hdd media.
    */
   private static DatanodeStorageReport generateStorageReport(int id, int cacheUsed,
         int cacheRemain) {

      DatanodeInfo dni = new DatanodeInfo("ip" + id, "host" + id, "dn" + id, 80, 81, 82, 83,
            40000 * MB, 10000 * MB, 30000 * MB, 10000 * MB, (cacheUsed + cacheRemain) * MB,
            cacheUsed * MB, cacheRemain * MB, cacheUsed * MB, 0, 0, 0, "default-rack", null);

      StorageReport srm1 = new StorageReport(
            new DatanodeStorage("sr" + id + "m1", State.NORMAL, StorageType.MEMORY), false,
            2000 * MB, 1000 * MB, 1000 * MB, 1000 * MB, 184320, 184320,
            (cacheUsed + cacheRemain) * MB, cacheUsed * MB, cacheRemain * MB, cacheUsed * MB, 0, 0);
      StorageReport srs1 = new StorageReport(
            new DatanodeStorage("sr" + id + "s1", State.NORMAL, StorageType.SSD), false, 8000 * MB,
            1000 * MB, 7000 * MB, 1000 * MB, 40960, 40960, 0, 0, 0, 0, 0, 0);
      StorageReport srd1 = new StorageReport(
            new DatanodeStorage("sr" + id + "d1", State.NORMAL, StorageType.DISK), false,
            15000 * MB, 4000 * MB, 11000 * MB, 4000 * MB, 16384, 16384, 0, 0, 0, 0, 0, 0);
      StorageReport srd2 = new StorageReport(
            new DatanodeStorage("sr" + id + "d2", State.NORMAL, StorageType.DISK), false,
            15000 * MB, 4000 * MB, 11000 * MB, 4000 * MB, 16384, 16384, 0, 0, 0, 0, 0, 0);

      DatanodeStorageReport dnsr = new DatanodeStorageReport(dni,
            new StorageReport[] { srm1, srs1, srd1, srd2 });

      return dnsr;
   }

   /**
    * @return a synthetic set of files with block locations
    */
   private static Collection<FileStatus> generateFiles(Random rnd, String path, int numFiles,
         int numBlocksPerFile, int numNodes, int numDisksPerNode, int lastBlockMB) {

      ArrayList<FileStatus> files = new ArrayList<FileStatus>(numFiles);

      // Generate nodes
      ArrayList<Integer> nodes = new ArrayList<Integer>(numNodes);
      for (int n = 1; n <= numNodes; ++n) {
         nodes.add(n);
      }

      long rv = ReplicationVector.ParseReplicationVector("D=3");

      // Generate files
      for (int f = 1; f <= numFiles; ++f) {
         BlockLocation[] blks = new BlockLocation[numBlocksPerFile];

         for (int b = 0; b < numBlocksPerFile; ++b) {
            Collections.shuffle(nodes, rnd);
            blks[b] = new BlockLocation(null,
                  new String[] { "host" + nodes.get(0), "host" + nodes.get(1),
                        "host" + nodes.get(2) },
                  new StorageType[] { StorageType.DISK, StorageType.DISK, StorageType.DISK }, null,
                  null, b * 128 * MB, (b < numBlocksPerFile - 1) ? 128 * MB : lastBlockMB * MB,
                  false,
                  new String[] { "sr" + nodes.get(0) + "d" + (1 + rnd.nextInt(numDisksPerNode)),
                        "sr" + nodes.get(1) + "d" + (1 + rnd.nextInt(numDisksPerNode)),
                        "sr" + nodes.get(2) + "d" + (1 + rnd.nextInt(numDisksPerNode)) },
                  null);
         }

         files.add(new LocatedFileStatus((numBlocksPerFile - 1) * 128 * MB + lastBlockMB * MB,
               false, rv, 128 * MB, 0, 0, null, "test", "test", null, new Path(path + "/f" + f),
               blks));
      }

      return files;
   }

   /**
    * @return a synthetic set of datanode information
    */
   private static DatanodeStorageReport[] generateStorageReports(Random rnd, int numNodes,
         int numDisksPerNode, long memSizePerNode, long diskSize) {

      DatanodeStorageReport[] dnsrs = new DatanodeStorageReport[numNodes];
      long capacity = (memSizePerNode + diskSize * numDisksPerNode) * MB;

      for (int n = 1; n <= numNodes; ++n) {
         // Generate datanode info
         DatanodeInfo dni = new DatanodeInfo("ip" + n, "host" + n, "dn" + n, 80, 81, 82, 83,
               capacity, 0, capacity, 0, memSizePerNode * MB, 0, memSizePerNode * MB, 0, 0, 0, 0,
               "default-rack", null);

         ArrayList<StorageReport> reports = new ArrayList<StorageReport>(numDisksPerNode + 1);

         // Generate one memory and multiple disk storage media
         reports.add(new StorageReport(
               new DatanodeStorage("sr" + n + "m1", State.NORMAL, StorageType.MEMORY), false,
               memSizePerNode * MB, 0, memSizePerNode * MB, 0, 184320, 184320, memSizePerNode * MB,
               0, memSizePerNode * MB, 0, 0, 0));

         for (int d = 1; d <= numDisksPerNode; ++d) {
            reports.add(new StorageReport(
                  new DatanodeStorage("sr" + n + "d" + d, State.NORMAL, StorageType.DISK), false,
                  diskSize * MB, 0, diskSize * MB, 0, 16384, 16384, 0, 0, 0, 0, 0, 0));
         }

         dnsrs[n - 1] = new DatanodeStorageReport(dni, reports.toArray(new StorageReport[] {}));
      }

      return dnsrs;
   }

}
