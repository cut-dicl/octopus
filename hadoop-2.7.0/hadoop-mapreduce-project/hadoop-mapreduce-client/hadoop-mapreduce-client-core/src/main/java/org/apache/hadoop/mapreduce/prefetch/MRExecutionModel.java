/**
 * 
 */
package org.apache.hadoop.mapreduce.prefetch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ReplicationVector;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.mapreduce.ClusterMetrics;

/**
 * This class models the MapReduce scheduling and execution of map tasks as well
 * as the prefetching/caching of tasks' input data.
 * 
 * @author hero
 */
public class MRExecutionModel {

   private static final Log LOG = LogFactory.getLog(MRExecutionModel.class);

   // One-time constants (all times in ms)
   private static boolean Initialized = false;
   private static long TimeToInit;
   private static long TimeToInitCache;
   private static long TimeToSchedule;
   private static long MaxTransfersPerHost;
   private static double WasteTolerance;
   static boolean EnablePartialCacheRead;
   private static EnumMap<StorageType, Double> Bandwidths;

   private HashMap<String, Long> cacheRemainPerHost;
   private Long totalCacheRemain;

   private int numAvailSlots;
   private double cacheDelay;

   /**
    * Constructor with various initializations
    * 
    * @param conf
    * @param metrics - info about YARN
    * @param report  - info about DFS
    * @throws IOException
    * @throws InterruptedException
    */
   public MRExecutionModel(Configuration conf, ClusterMetrics metrics,
         DatanodeStorageReport[] report) throws IOException, InterruptedException {

      InitializeConstants(conf, report);

      // Compute the cache remaining per host
      cacheRemainPerHost = new HashMap<String, Long>();
      totalCacheRemain = 0l;

      for (DatanodeStorageReport dns : report) {
         for (StorageReport sr : dns.getStorageReports()) {
            if (sr.getStorage().getStorageType() == StorageType.MEMORY) {
               cacheRemainPerHost.put(dns.getDatanodeInfo().getHostName(), sr.getCacheRemaining());
               totalCacheRemain += sr.getCacheRemaining();
            }
         }
      }

      // Compute number of available slots
      int coresPerMap = conf.getInt("mapreduce.map.cpu.vcores", 1);
      int memPerMap = conf.getInt("mapreduce.map.memory.mb", 512);

      int coresAvail = metrics.getAvailableCores();
      int memAvailMB = metrics.getAvailableMemory();

      numAvailSlots = Math.min(coresAvail / coresPerMap, memAvailMB / memPerMap);
      numAvailSlots -= 1; // For AM
      if (numAvailSlots <= 0)
         numAvailSlots = 2;

      cacheDelay = 0d;
   }

   public void setCacheDelay(double cacheDelay) {
      this.cacheDelay = cacheDelay;
   }

   /**
    * Generate a list of task models given a list of files. A task model is created
    * for each file block. Each task model will contain the block's length, the
    * storage type of its base replica, and the locations (i.e., host-type pairs)
    * of its replicas. The tasks are returned sorted by block length descending.
    * 
    * @param files
    * @param conf
    * @return
    */
   public ArrayList<MRTaskModel> generateMRTaskModels(Collection<FileStatus> files,
         Configuration conf) {

      // Create a task for each file block
      ArrayList<MRTaskModel> tasks = new ArrayList<MRTaskModel>();
      for (FileStatus file : files) {
         Path path = file.getPath();
         long length = file.getLen();
         if (length > 0) {
            // Get block locations
            BlockLocation[] blkLocations = null;
            if (file instanceof LocatedFileStatus) {
               blkLocations = ((LocatedFileStatus) file).getBlockLocations();
            } else {
               try {
                  FileSystem fs = path.getFileSystem(conf);
                  blkLocations = fs.getFileBlockLocations(file, 0, length);
               } catch (IOException e) {
                  LOG.warn("Failed to get block locations for " + path, e);
               }
            }

            StorageType type = ReplicationVector.getHighestStorageType(file.getReplicationVector());
            if (blkLocations == null) {
               // Create a task for the entire file
               tasks.add(new MRTaskModel(file, length, null, null, null, type, false));
            } else {
               // Create a task for each block
               for (BlockLocation blk : blkLocations) {
                  tasks.add(
                        new MRTaskModel(file, blk.getLength(), blk.getHosts(), blk.getStorageIDs(),
                              blk.getStorageTypes(), type, blk.getCachedHosts().length > 0));
               }
            }
         }
      }

      // Sort tasks be length decreasing
      tasks.sort(new MRTaskModel.MRTaskComparatorByLengthDecr());

      return tasks;
   }

   /**
    * Find which files can be cached given cache capacity constraints. For each
    * task corresponding to one of these files, find and set the optimal location
    * (host-type pair) for caching.
    * 
    * Selects a list of files to cache and stores it in the filesToCache parameter.
    * 
    * @param filesToTasks - mapping from files to tasks
    * @param filesToCache - list to be completed by this methods
    * @param maxCacheSize - max size for caching
    * @param maxCacheDoP  - max degree of parallelism for caching (per media)
    * @return return true if prefetching a file violates maxCacheDoP
    */
   public boolean selectFilesForPrefetching(Map<FileStatus, ArrayList<MRTaskModel>> filesToTasks,
         HashSet<FileStatus> filesToCache, long maxCacheSize, long maxCacheDoP) {

      // Select which files to cache
      HashMap<String, Long> cacheUsedPerHost = new HashMap<String, Long>();
      HashMap<String, Integer> numTasksPerHost = new HashMap<String, Integer>();
      HashMap<String, Integer> numTasksPerStorage = new HashMap<String, Integer>();
      long totalLength = 0l, totalWasted = 0l;
      boolean globalInvalid = false;
      long maxLength = Math.min(totalCacheRemain, maxCacheSize);

      for (Entry<FileStatus, ArrayList<MRTaskModel>> entry : filesToTasks.entrySet()) {
         FileStatus file = entry.getKey();
         ArrayList<MRTaskModel> tasks = entry.getValue();

         long length = file.getLen();
         short mem = ReplicationVector.GetReplication(StorageType.MEMORY,
               file.getReplicationVector());
         if (mem > 0 || length <= 0l || totalLength + length > maxLength)
            continue; // Move on to the next file

         // Find the best candidate cache location for each task
         for (MRTaskModel task : tasks) {
            findCandidateCacheLoc(task, cacheUsedPerHost, numTasksPerHost, numTasksPerStorage);
         }

         // Validate that the cache locations respect the max cache DoP
         boolean invalid = false;
         for (Integer count : numTasksPerStorage.values()) {
            if (count > maxCacheDoP) {
               invalid = true;
               globalInvalid = true;
               break;
            }
         }

         // Validate that the cache locations respect the max transfers per host
         long wasted = 0l;
         if (!invalid) {
            for (String host : numTasksPerHost.keySet()) {
               if (numTasksPerHost.get(host) > MaxTransfersPerHost) {
                  for (MRTaskModel task : tasks) {
                     if (task.hasCandidateCacheLoc() && task.getCandidateCacheHost().equals(host)) {
                        wasted += task.getLength();
                     }
                  }
               }
            }

            if (wasted > 0) {
               double cached = totalLength;
               for (MRTaskModel task : tasks) {
                  if (task.hasCandidateCacheLoc()) {
                     cached += task.getLength();
                  }
               }

               if ((totalWasted + wasted) / cached >= WasteTolerance) {
                  invalid = true;
               }
            }
         }

         // If invalid, undo the changes
         if (invalid) {
            for (MRTaskModel task : tasks) {
               if (task.hasCandidateCacheLoc()) {
                  numTasksPerStorage.put(task.getCandidateCacheStorageID(),
                        numTasksPerStorage.get(task.getCandidateCacheStorageID()) - 1);
                  numTasksPerHost.put(task.getCandidateCacheHost(),
                        numTasksPerHost.get(task.getCandidateCacheHost()) - 1);
                  cacheUsedPerHost.put(task.getCandidateCacheHost(),
                        cacheUsedPerHost.get(task.getCandidateCacheHost()) - task.getLength());
                  task.setCandidateCacheLocation(-1);
               }
            }
         } else {
            // Select this file
            totalWasted += wasted;
            totalLength += length;
            filesToCache.add(file);
         }
      }

      return globalInvalid;
   }

   /**
    * Find the best candidate cache location, i.e., the location with the lowest
    * storage type and the highest cache remaining space, and set it in the task.
    * 
    * Note: all 4 parameters might get modified by this call
    * 
    * @param task
    * @param cacheUsedPerHost
    * @param numTasksPerHost
    * @param numTasksPerStorage
    * @return
    */
   private void findCandidateCacheLoc(MRTaskModel task, HashMap<String, Long> cacheUsedPerHost,
         HashMap<String, Integer> numTasksPerHost, HashMap<String, Integer> numTasksPerStorage) {

      String[] hosts = task.getHosts();
      StorageType[] types = task.getStorageTypes();
      long cacheRemain;

      if (task.isAlreadyCached() || hosts == null || types == null || hosts.length != types.length)
         return;

      int candIndex = -1;
      String candHost = null;
      StorageType candType = StorageType.MEMORY;
      long candCacheRemain = -1l;

      for (int i = 0; i < hosts.length; ++i) {
         if (types[i].getOrderValue() > candType.getOrderValue()) {
            // Found better location (i.e., lower storage type)
            candIndex = i;
            candHost = hosts[i];
            candType = types[i];
            candCacheRemain = computeCacheRemain(hosts[i], cacheRemainPerHost, cacheUsedPerHost);
         } else if (types[i].getOrderValue() == candType.getOrderValue()) {

            cacheRemain = computeCacheRemain(hosts[i], cacheRemainPerHost, cacheUsedPerHost);
            if (cacheRemain > candCacheRemain) {
               // Found better location (i.e., with higher cache remaining space)
               candIndex = i;
               candHost = hosts[i];
               candCacheRemain = cacheRemain;
            }
         }
      }

      if (candHost != null) {
         // Found candidate cache location
         task.setCandidateCacheLocation(candIndex);

         // Update cache used
         if (!cacheUsedPerHost.containsKey(candHost))
            cacheUsedPerHost.put(candHost, task.getLength());
         else
            cacheUsedPerHost.put(candHost, cacheUsedPerHost.get(candHost) + task.getLength());

         // Update host counts
         if (!numTasksPerHost.containsKey(candHost))
            numTasksPerHost.put(candHost, 1);
         else
            numTasksPerHost.put(candHost, numTasksPerHost.get(candHost) + 1);

         // Update storage counts
         String storageID = task.getCandidateCacheStorageID();
         if (!numTasksPerStorage.containsKey(storageID))
            numTasksPerStorage.put(storageID, 1);
         else
            numTasksPerStorage.put(storageID, numTasksPerStorage.get(storageID) + 1);
      }
   }

   /**
    * Simple method to compute the updated remaining cache of a host
    * 
    * @param host
    * @param cacheRemainPerHost
    * @param cacheUsedPerHost
    * @return
    */
   private long computeCacheRemain(String host, HashMap<String, Long> cacheRemainPerHost,
         HashMap<String, Long> cacheUsedPerHost) {

      long cacheRemain = (cacheRemainPerHost.containsKey(host)) ? cacheRemainPerHost.get(host) : 0l;
      long cacheUsed = (cacheUsedPerHost.containsKey(host)) ? cacheUsedPerHost.get(host) : 0l;

      return (cacheRemain > cacheUsed) ? cacheRemain - cacheUsed : 0l;
   }

   /**
    * Simulate the execution of tasks on the available slots. This method will set
    * the expected start time and end time of each task model based on scheduling
    * and storage type to read from.
    * 
    * Computes and returns the latest end time of all tasks.
    * 
    * @param tasks
    * @return
    */
   public double simulateMRTaskExecution(ArrayList<MRTaskModel> tasks) {

      PriorityQueue<Double> endTimes = new PriorityQueue<Double>(numAvailSlots);
      for (int i = 0; i < numAvailSlots; ++i)
         endTimes.add(0d);

      // Schedule each task after the earliest task end time
      double currEndTime, prevEndTime, maxEndTime = 0d;
      for (MRTaskModel task : tasks) {
         prevEndTime = endTimes.poll();

         // Set start time
         if (prevEndTime == 0d)
            task.setStartTime(cacheDelay + TimeToInit + TimeToSchedule); // first wave
         else
            task.setStartTime(prevEndTime + TimeToSchedule);

         // Set end time
         currEndTime = task.getStartTime() + 1000 + computeReadDuration(task)
               + 2000d * task.getLength() / 134217728.0;
         task.setEndTime(currEndTime);

         endTimes.add(currEndTime);
         if (currEndTime > maxEndTime)
            maxEndTime = currEndTime;
      }

      return maxEndTime;
   }

   /**
    * Compute the read duration for this task. This method takes into consideration
    * where the data is read from (cache, base, or partially from both)
    * 
    * @param task
    * @return
    */
   private double computeReadDuration(MRTaskModel task) {

      double duration;

      if (task.isReadFullyFromCache()) {
         // Read from cache
         duration = task.getLength() / Bandwidths.get(StorageType.MEMORY);
      } else if (task.isReadPartiallyFromCache()) {
         // First part is read from cache and second part from base storage (plus cache)
         double ratio = (task.getStartTime() - TimeToInitCache)
               / (task.getCacheTime() - TimeToInitCache);
         duration = task.getLength() / Bandwidths.get(StorageType.MEMORY);
         duration += (1 - ratio) * task.getLength() / Bandwidths.get(task.getBaseStorageType());
      } else {
         // Read from base storage
         duration = task.getLength() / Bandwidths.get(task.getBaseStorageType());
      }

      return duration;
   }

   /**
    * Simulate the caching of the tasks' input. This method will set the expected
    * time (if any) when caching will complete for each task model.
    * 
    * Caching happens using concurrent threads. Hence, caching more than one block
    * per source media will lead to contention.
    * 
    * ASSUMES that {@link #selectFilesForPrefetching(Map, HashSet, long, long)} has
    * been called.
    * 
    * @param tasks
    */
   public void simulateMRTaskCaching(ArrayList<MRTaskModel> tasks) {

      HashMap<String, HashMap<String, ArrayList<MRTaskModel>>> tasksPerStorPerHost; // host->stor->tasks
      HashMap<String, ArrayList<MRTaskModel>> tasksPerStor; // stor->tasks
      HashMap<String, Integer> numTasksPerHost; // host->num_tasks
      String host, storID;

      // Group tasks per candidate cache storage per node
      tasksPerStorPerHost = new HashMap<String, HashMap<String, ArrayList<MRTaskModel>>>();
      numTasksPerHost = new HashMap<String, Integer>();

      for (MRTaskModel task : tasks) {
         if (task.getBaseStorageType() != StorageType.MEMORY && !task.isAlreadyCached()
               && task.hasCandidateCacheLoc()) {
            host = task.getCandidateCacheHost();
            if (!tasksPerStorPerHost.containsKey(host)) {
               tasksPerStorPerHost.put(host, new HashMap<String, ArrayList<MRTaskModel>>());
               numTasksPerHost.put(host, 0);
            }

            if (numTasksPerHost.get(host) < MaxTransfersPerHost) {
               tasksPerStor = tasksPerStorPerHost.get(host);
               storID = task.getCandidateCacheStorageID();
               if (!tasksPerStor.containsKey(storID)) {
                  tasksPerStor.put(storID, new ArrayList<MRTaskModel>());
               }

               tasksPerStor.get(storID).add(task);
               numTasksPerHost.put(host, numTasksPerHost.get(host) + 1);
            }
         }
      }

      // Compute the expected cache time
      for (HashMap<String, ArrayList<MRTaskModel>> tasksPerStorage : tasksPerStorPerHost.values()) {
         for (ArrayList<MRTaskModel> tasksOnStorage : tasksPerStorage.values()) {
            if (tasksOnStorage.size() == 1) {
               // Only one task on this host
               MRTaskModel task = tasksOnStorage.get(0);
               task.setCacheTime(TimeToInitCache
                     + (task.getLength() / Bandwidths.get(task.getCandidateCacheType())));
            } else {
               // Multiple tasks on this host - sort on increasing length
               tasksOnStorage.sort(new MRTaskModel.MRTaskComparatorByLengthIncr());

               int numTasks = tasksOnStorage.size();
               long prevLength = 0l, currLength;
               double prevCacheTime = 0d, currCacheTime;

               for (MRTaskModel task : tasksOnStorage) {
                  currLength = task.getLength() - prevLength;
                  currCacheTime = prevCacheTime
                        + numTasks * currLength / Bandwidths.get(task.getCandidateCacheType());

                  task.setCacheTime(TimeToInitCache + currCacheTime);

                  prevLength = task.getLength();
                  prevCacheTime = currCacheTime;
                  numTasks -= 1;
               }

               // OLD APPROACH
               // ECT_t = 1/2*(duration_t + sum_j_in_N[t]{duration_j})
               // where
               // duration_t = time to cache input of t based on the storage type of the
               // replica
               // N[t] = source storage media for caching t
               //
               // double totalCacheTime = 0d;
               // for (MRTaskModel task : tasksOnStorage) {
               // totalCacheTime += task.getLength() /
               // Bandwidths.get(task.getCandidateCacheType());
               // }
               //
               // for (MRTaskModel task : tasksOnStorage) {
               // task.setCacheTime(
               // 500 + (((task.getLength() / Bandwidths.get(task.getCandidateCacheType()))
               // + totalCacheTime) / 2));
               // }
            }
         }
      }
   }

   /**
    * Returns a list of files that will be read from cache based on the latest
    * invocation of the {@link #simulateMRTaskExecution(ArrayList)}.
    * 
    * ASSUMES that both {@link #simulateMRTaskCaching(ArrayList)} and
    * {@link #simulateMRTaskExecution(ArrayList)} have been executed before.
    * 
    * @param tasks
    * @return
    */
   public HashSet<FileStatus> getFilesReadFromCache(ArrayList<MRTaskModel> tasks) {

      HashSet<FileStatus> filesFromCache = new HashSet<FileStatus>();
      for (MRTaskModel task : tasks) {
         if (task.isReadFullyFromCache())
            filesFromCache.add(task.getFileStatus());
      }

      return filesFromCache;
   }

   /**
    * Find the maximum delay to ensure that all tasks will read from the cache.
    * 
    * ASSUMES that both {@link #simulateMRTaskCaching(ArrayList)} and
    * {@link #simulateMRTaskExecution(ArrayList)} have been executed before.
    * 
    * @param tasks
    * @return
    */
   public double findMaxCacheDelay(ArrayList<MRTaskModel> tasks) {

      int i = 1;
      double cacheDelay, maxCacheDelay = 0d;
      for (MRTaskModel task : tasks) {
         if (task.getCacheTime() > 0) {
            // Compute needed cache delay
            cacheDelay = task.getCacheTime() - task.getStartTime();
            if (cacheDelay > maxCacheDelay)
               maxCacheDelay = cacheDelay;
         }

         if (i % numAvailSlots == 0 && maxCacheDelay > 0)
            return maxCacheDelay; // Early termination for efficiency
         ++i;
      }

      return maxCacheDelay;
   }

   /**
    * Initialize constant (static) values
    * 
    * @param conf
    * @param report
    */
   private static void InitializeConstants(Configuration conf, DatanodeStorageReport[] report) {
      if (Initialized == false) {
         TimeToInit = 3000l
               + conf.getLong("yarn.resourcemanager.nodemanagers.heartbeat-interval-ms", 1000l);
         TimeToSchedule = conf.getLong("yarn.app.mapreduce.am.scheduler.heartbeat.interval-ms",
               1000l);
         MaxTransfersPerHost = conf.getInt("dfs.namenode.replication.max-streams", 4);
         WasteTolerance = conf.getDouble("mapreduce.octopus.prefetcher.waste.tolerance", 0.05);
         EnablePartialCacheRead = conf.getBoolean("dfs.datanode.enable.partial.cache.read", false);
         TimeToInitCache = 500l;

         // Compute the average bandwidth per storage type from the storage report
         Bandwidths = new EnumMap<StorageType, Double>(StorageType.class);
         EnumMap<StorageType, Integer> counts = new EnumMap<StorageType, Integer>(
               StorageType.class);

         for (StorageType type : StorageType.values()) {
            Bandwidths.put(type, 0d);
            counts.put(type, 0);
         }

         StorageType st;
         for (DatanodeStorageReport dns : report) {
            for (StorageReport sr : dns.getStorageReports()) {
               st = sr.getStorage().getStorageType();
               Bandwidths.put(st, Bandwidths.get(st) + sr.getReadThroughput());
               counts.put(st, counts.get(st) + 1);
            }
         }

         for (StorageType type : StorageType.values()) {
            if (counts.get(type) > 0) // Convert KB/s to B/s
               Bandwidths.put(type, 1024 * Bandwidths.get(type) / counts.get(type));
         }

         // If no disk bandwidth is found, then fix the values
         if (Bandwidths.get(StorageType.DISK) == 0d) {
            Bandwidths.put(StorageType.MEMORY, 167772160d);
            Bandwidths.put(StorageType.RAM_DISK, 117440512d);
            Bandwidths.put(StorageType.SSD_H, 75497472d);
            Bandwidths.put(StorageType.SSD, 50331648d);
            Bandwidths.put(StorageType.DISK_H, 25165824d);
            Bandwidths.put(StorageType.DISK, 16777216d);
            Bandwidths.put(StorageType.ARCHIVE, 8388608d);
            Bandwidths.put(StorageType.REMOTE, 4194304d);
         } else {
            // Set relative values compared to disk
            if (Bandwidths.get(StorageType.MEMORY) == 0l)
               Bandwidths.put(StorageType.MEMORY, 10 * Bandwidths.get(StorageType.DISK));
            if (Bandwidths.get(StorageType.RAM_DISK) == 0l)
               Bandwidths.put(StorageType.RAM_DISK, 7 * Bandwidths.get(StorageType.DISK));
            if (Bandwidths.get(StorageType.SSD_H) == 0l)
               Bandwidths.put(StorageType.SSD_H, 4 * Bandwidths.get(StorageType.DISK));
            if (Bandwidths.get(StorageType.SSD) == 0l)
               Bandwidths.put(StorageType.SSD, 3 * Bandwidths.get(StorageType.DISK));
            if (Bandwidths.get(StorageType.DISK_H) == 0l)
               Bandwidths.put(StorageType.DISK_H, (long) 1.5 * Bandwidths.get(StorageType.DISK));
            if (Bandwidths.get(StorageType.ARCHIVE) == 0l)
               Bandwidths.put(StorageType.ARCHIVE, Bandwidths.get(StorageType.DISK) / 2);
            if (Bandwidths.get(StorageType.REMOTE) == 0l)
               Bandwidths.put(StorageType.REMOTE, Bandwidths.get(StorageType.DISK) / 4);
         }

         // Convert bytes/sec to bytes/ms
         for (StorageType type : StorageType.values()) {
            Bandwidths.put(type, Bandwidths.get(type) / 1000);
         }

         Initialized = true;
      }
   }

}
