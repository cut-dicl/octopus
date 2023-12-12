/**
 * 
 */
package org.apache.hadoop.mapreduce.prefetch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.ReplicationVector;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.util.OctopusCache;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * This class guides the Octopus prefetching features and implements two
 * prefetching strategies. The first strategy simply prefetches all possible
 * files into the cache. The second strategy will select a set of files and
 * prefetch them only if it computes that prefetching will reduce the execution
 * time of the job (specifically, the map phase of the job). There is also an
 * optional feature for computing and introducing a caching delay to give time
 * to the file system to cache the data before the tasks are submitted for
 * execution.
 * 
 * @author hero
 */
public class MRPrefetcher {

   private static final Log LOG = LogFactory.getLog(MRPrefetcher.class);

   static {
      Logger.getLogger(MRPrefetcher.class).setLevel(Level.DEBUG);
   }

   private static final String OCTOPUS_PREFETCHER_STRATEGY = "mapreduce.octopus.prefetcher.strategy";
   private static final String OCTOPUS_PREFETCHER_MAX_WAIT = "mapreduce.octopus.prefetcher.max.wait.ms";
   private static final String OCTOPUS_PREFETCHER_MAX_SIZE = "mapreduce.octopus.prefetcher.max.size";
   private static final String OCTOPUS_PREFETCHER_EXPIRY = "mapreduce.octopus.prefetcher.expiry.ms";
   private static final String OCTOPUS_PREFETCHER_MIN_SPEEDUP = "mapreduce.octopus.prefetcher.min.speedup";
   private static final String OCTOPUS_PREFETCHER_WASTE_TOLERANCE = "mapreduce.octopus.prefetcher.waste.tolerance";

   private static final String OCTOPUS_PREFETCHER_PREFETCHED_IDS = "mapreduce.octopus.prefetcher.prefetched.ids";

   /**
    * Check if prefetching is enabled (based on the configured strategy)
    * 
    * @param conf
    * @return
    */
   public static boolean isPrefetchingEnabled(Configuration conf) {
      return conf.getInt(OCTOPUS_PREFETCHER_STRATEGY, 0) > 0;
   }

   /**
    * Prefetch the input files based on the configured prefetching strategy
    * 
    * @param files
    * @param cluster
    * @param conf
    */
   public static void prefetchFiles(Collection<FileStatus> files, Cluster cluster,
         Configuration conf) {
      DistributedFileSystem dfs = getDFS(conf);
      if (dfs == null)
         return;

      int strategy = conf.getInt(OCTOPUS_PREFETCHER_STRATEGY, 0);
      long maxWait = conf.getLong(OCTOPUS_PREFETCHER_MAX_WAIT, 0l);
      long maxSize = conf.getLongBytes(OCTOPUS_PREFETCHER_MAX_SIZE, Long.MAX_VALUE);

      Collection<Long> ids = null;

      switch (strategy) {
      case 0:
         // Nothing to do
         break;
      case 1:
         // Prefetch as much as possible
         ids = prefetchAllFiles(files, dfs, maxWait, maxSize);
         break;
      case 2:
         // Select and prefetch the most promising files
         try {
            ClusterMetrics metrics = cluster.getClusterStatus();
            DatanodeStorageReport[] report = getDFSStorageReport(dfs);
            ids = prefetchSelectiveFiles(files, metrics, report, conf, dfs, maxWait, maxSize);
         } catch (IOException | InterruptedException e) {
            LOG.error("Unable to perform selective caching, switching to full caching.", e);
            ids = prefetchAllFiles(files, dfs, maxWait, maxSize);
         }
         break;
      default:
         LOG.warn("Invalid octopus prefetching strategy: " + strategy);
         break;
      }

      // Set the cache directive IDs
      if (ids != null && !ids.isEmpty()) {
         conf.set(OCTOPUS_PREFETCHER_PREFETCHED_IDS, ids.toString());
      }
   }

   /**
    * Prefetch all files that are not in memory and respect the max size
    * 
    * @param files
    * @param dfs
    * @param maxWait
    * @param maxSize
    * @return the list of cache directive IDs (or null)
    */
   public static Collection<Long> prefetchAllFiles(Collection<FileStatus> files,
         DistributedFileSystem dfs, long maxWait, long maxSize) {

      HashSet<FileStatus> filesToPrefetch = new HashSet<FileStatus>(files.size());
      long length, totalLength = 0;
      short mem;

      // Select all files that are not in memory and respect maxSize
      for (FileStatus file : files) {
         length = file.getLen();
         mem = ReplicationVector.GetReplication(StorageType.MEMORY, file.getReplicationVector());
         if (mem == 0 && length > 0l && totalLength + length < maxSize) {
            totalLength += length;
            filesToPrefetch.add(file);
         }
      }

      LOG.info(String.format(
            "Prefetching files=%d blocks=%d length=%d allfiles=%d allblocks=%d alllength=%d delay=%d",
            filesToPrefetch.size(), computeNumberOfBlocks(filesToPrefetch), totalLength,
            files.size(), computeNumberOfBlocks(files), computeTotalFileLength(files), maxWait));

      // Prefetch the selected files
      if (filesToPrefetch.size() > 0)
         return OctopusCache.addCachePoolAndDirectives(dfs, filesToPrefetch);
      else
         return null;
   }

   /**
    * Identifies candidate files for prefetching and orders prefetching only if
    * there is benefit (in terms of reduced execution time for the job's map
    * phase).
    * 
    * The key idea is to simulate the execution of tasks without and with
    * prefetching of files. If the estimated speedup is larger than a threshold,
    * then prefetch the candidate files.
    * 
    * There is also an optional feature for computing and introducing a caching
    * delay to give time to the file system to cache the data before the tasks are
    * submitted for execution.
    * 
    * @param files
    * @param metrics
    * @param report
    * @param conf
    * @param dfs
    * @param maxWait
    * @param maxSize
    * @return the list of cache directive IDs (or null)
    * @throws IOException
    * @throws InterruptedException
    */
   public static Collection<Long> prefetchSelectiveFiles(Collection<FileStatus> files,
         ClusterMetrics metrics, DatanodeStorageReport[] report, Configuration conf,
         DistributedFileSystem dfs, long maxWait, long maxSize)
         throws IOException, InterruptedException {

      boolean enablePartialCacheRead = conf.getBoolean("dfs.datanode.enable.partial.cache.read",
            false);

      MRExecutionModel execModel = new MRExecutionModel(conf, metrics, report);

      // Generate the tasks
      ArrayList<MRTaskModel> tasks = execModel.generateMRTaskModels(files, conf);

      TreeMap<FileStatus, ArrayList<MRTaskModel>> filesToTasks = new TreeMap<FileStatus, ArrayList<MRTaskModel>>(
            enablePartialCacheRead ? new FileStatusComparatorByLengthDecr()
                  : new FileStatusComparatorByLengthIncr());
      for (MRTaskModel task : tasks) {
         if (!filesToTasks.containsKey(task.getFileStatus()))
            filesToTasks.put(task.getFileStatus(), new ArrayList<MRTaskModel>());
         filesToTasks.get(task.getFileStatus()).add(task);
      }

      // Simulate execution (without prefetching)
      double execTimeNoCache = execModel.simulateMRTaskExecution(tasks);

      if (LOG.isDebugEnabled()) {
         LOG.debug(String.format("Simulated MR execution without prefetching: time=%.0f",
               execTimeNoCache));
         for (MRTaskModel task : tasks)
            LOG.debug(task.toString());
      }

      double bestExecTimeCache = Double.MAX_VALUE;
      long bestCacheDelay = 0l;
      boolean bestUtilization = false;
      HashSet<FileStatus> bestFilesToPrefetch = null;
      HashSet<FileStatus> previousFilesToPrefetch = null;
      double wasteTol = conf.getDouble(OCTOPUS_PREFETCHER_WASTE_TOLERANCE, 0.05);

      int maxCacheDoP = 1;
      for (; maxCacheDoP <= tasks.size(); ++maxCacheDoP) {

         // Reset caching-related parameters of the tasks
         if (maxCacheDoP > 1) {
            for (MRTaskModel task : tasks) {
               task.resetCaching();
            }
         }

         // Select which files to prefetch (that satisfy the given constraints)
         HashSet<FileStatus> filesToPrefetch = new HashSet<FileStatus>(filesToTasks.size());
         boolean invalid = execModel.selectFilesForPrefetching(filesToTasks, filesToPrefetch,
               maxSize, maxCacheDoP);

         if (filesToPrefetch.isEmpty() || (previousFilesToPrefetch != null
               && previousFilesToPrefetch.equals(filesToPrefetch))) {
            if (invalid || previousFilesToPrefetch == null)
               continue; // DoP too strict - move to higher values
            else
               break; // DoP does not impact selection - stop
         }

         // Simulate prefetching/caching and the execution
         long cacheDelay = (maxWait >= 0l) ? maxWait : 0;
         execModel.simulateMRTaskCaching(tasks);
         execModel.setCacheDelay(cacheDelay);
         double execTimeCache = execModel.simulateMRTaskExecution(tasks);

         if (LOG.isDebugEnabled()) {
            LOG.debug(String.format(
                  "Simulated MR execution (dop=%d) with prefetching %d files and delay=%d: time=%.0f",
                  maxCacheDoP, filesToPrefetch.size(), cacheDelay, execTimeCache));
            for (MRTaskModel task : tasks)
               LOG.debug(task.toString());
         }

         if (maxWait < 0l) {
            // Find the max cache delay and simulate execution
            double newCacheDelay = execModel.findMaxCacheDelay(tasks);
            if (newCacheDelay > 0 && newCacheDelay < bestExecTimeCache) {
               execModel.setCacheDelay(newCacheDelay);
               double execTimeCacheDelay = execModel.simulateMRTaskExecution(tasks);

               if (LOG.isDebugEnabled()) {
                  LOG.debug(String.format(
                        "Simulated MR execution (dop=%d) with prefetching %d files and delay=%d: time=%.0f",
                        maxCacheDoP, filesToPrefetch.size(), Math.round(newCacheDelay),
                        execTimeCacheDelay));
                  for (MRTaskModel task : tasks)
                     LOG.debug(task.toString());
               }

               if (execTimeCacheDelay <= execTimeCache) {
                  // Delay is beneficial
                  execTimeCache = execTimeCacheDelay;
                  cacheDelay = Math.round(newCacheDelay);
               } else if (execTimeCache <= bestExecTimeCache) {
                  // Delay is not beneficial - need to restore the tasks
                  execModel.setCacheDelay(0);
                  execModel.simulateMRTaskExecution(tasks);
               }
            }
         }

         // Check if any of the prefetched files offer no benefit to job execution
         HashSet<FileStatus> filesNotToPrefetch = new HashSet<FileStatus>(filesToPrefetch.size());
         for (FileStatus file : filesToPrefetch) {
            if (!isPrefetchedFileUseful(file, filesToTasks)) {
               // Do NOT prefetch this file
               filesNotToPrefetch.add(file);
               if (filesToTasks.containsKey(file))
                  for (MRTaskModel task : filesToTasks.get(file))
                     task.resetCaching();
            }
         }

         if (!filesNotToPrefetch.isEmpty()) {
            filesToPrefetch.removeAll(filesNotToPrefetch);

            // Simulate again caching and execution with new set of files
            if (!filesToPrefetch.isEmpty()) {
               execModel.simulateMRTaskCaching(tasks);
               execModel.setCacheDelay(cacheDelay);
               execTimeCache = execModel.simulateMRTaskExecution(tasks);

               if (LOG.isDebugEnabled()) {
                  LOG.debug(String.format(
                        "Simulated MR execution (dop=%d) with prefetching %d files and delay=%d: time=%.0f",
                        maxCacheDoP, filesToPrefetch.size(), cacheDelay, execTimeCache));
                  for (MRTaskModel task : tasks)
                     LOG.debug(task.toString());
               }
            } else {
               LOG.debug("None of the prefetched files offer benefits");
            }
         }

         if (bestFilesToPrefetch == null || execTimeCache < bestExecTimeCache) {
            // Found better settings - set bestUtilization first
            if (bestFilesToPrefetch == null && enablePartialCacheRead == true)
               bestUtilization = true;
            else
               bestUtilization = !isCacheWasted(tasks, wasteTol, enablePartialCacheRead);

            bestFilesToPrefetch = filesToPrefetch;
            bestExecTimeCache = execTimeCache;
            bestCacheDelay = cacheDelay;

         } else if (execTimeCache == bestExecTimeCache) {
            // Same runtime but more files cached => better cluster utilization
            // BUT: be careful not to prefetch more files that can be used
            if (!isCacheWasted(tasks, wasteTol, enablePartialCacheRead)
                  && filesToPrefetch.size() > bestFilesToPrefetch.size()) {
               // Found better settings
               bestFilesToPrefetch = filesToPrefetch;
               bestExecTimeCache = execTimeCache;
               bestCacheDelay = cacheDelay;
               bestUtilization = true;
            }
         }

         if (bestFilesToPrefetch.size() == files.size())
            break; // Early termination since all files should be cached

         if (!invalid && previousFilesToPrefetch != null
               && previousFilesToPrefetch.equals(filesToPrefetch)) {
            break; // Early termination as we are testing the same files again
         }

         previousFilesToPrefetch = filesToPrefetch;
      }

      LOG.info("MaxCacheDoP: " + maxCacheDoP);
      
      // Decide whether to cache the files or not
      double speedup = conf.getDouble(OCTOPUS_PREFETCHER_MIN_SPEEDUP, 1);
      if (bestFilesToPrefetch != null && execTimeNoCache / bestExecTimeCache > speedup) {
         // Prefetch selected files to improve running time
         Collection<FileStatus> filesInCache = getFilesAlreadyInCache(bestFilesToPrefetch,
               filesToTasks);
         LOG.info(String.format("Prefetching files=%d blocks=%d length=%d "
               + "acfiles=%d acblocks=%d aclength=%d allfiles=%d allblocks=%d alllength=%d delay=%d e1=%.0f e2=%.0f",
               bestFilesToPrefetch.size(), computeNumberOfBlocks(bestFilesToPrefetch, filesToTasks),
               computeTotalFileLength(bestFilesToPrefetch), filesInCache.size(),
               computeNumberOfBlocks(filesInCache, filesToTasks),
               computeTotalFileLength(filesInCache), files.size(), tasks.size(),
               computeTotalFileLength(files), bestCacheDelay, execTimeNoCache, bestExecTimeCache));
         conf.setLong(OCTOPUS_PREFETCHER_MAX_WAIT, bestCacheDelay);
         if (dfs != null)
            return OctopusCache.addCachePoolAndDirectives(dfs, bestFilesToPrefetch);
         else
            return null;

      } else if (bestFilesToPrefetch != null && execTimeNoCache / bestExecTimeCache == speedup
            && bestUtilization) {
         // Prefetch selected files to improve cluster utilization (not running time)
         Collection<FileStatus> filesInCache = getFilesAlreadyInCache(bestFilesToPrefetch,
               filesToTasks);
         LOG.info(String.format("Util Prefetching files=%d blocks=%d length=%d "
               + "acfiles=%d acblocks=%d aclength=%d allfiles=%d allblocks=%d alllength=%d delay=%d e1=%.0f e2=%.0f",
               bestFilesToPrefetch.size(), computeNumberOfBlocks(bestFilesToPrefetch, filesToTasks),
               computeTotalFileLength(bestFilesToPrefetch), filesInCache.size(),
               computeNumberOfBlocks(filesInCache, filesToTasks),
               computeTotalFileLength(filesInCache), files.size(), tasks.size(),
               computeTotalFileLength(files), bestCacheDelay, execTimeNoCache, bestExecTimeCache));
         conf.setLong(OCTOPUS_PREFETCHER_MAX_WAIT, bestCacheDelay);
         if (dfs != null)
            return OctopusCache.addCachePoolAndDirectives(dfs, bestFilesToPrefetch);
         else
            return null;

      } else {
         if (bestFilesToPrefetch != null) {
            // Special case: Call prefetching for files already cached (if any)
            Collection<FileStatus> filesInCache = getFilesAlreadyInCache(bestFilesToPrefetch,
                  filesToTasks);
            if (!filesInCache.isEmpty()) {
               LOG.info(String.format("Already Prefetching files=%d blocks=%d length=%d "
                     + "acfiles=%d acblocks=%d aclength=%d allfiles=%d allblocks=%d alllength=%d delay=0 e1=%.0f e2=%.0f",
                     filesInCache.size(), computeNumberOfBlocks(filesInCache, filesToTasks),
                     computeTotalFileLength(filesInCache), filesInCache.size(),
                     computeNumberOfBlocks(filesInCache, filesToTasks),
                     computeTotalFileLength(filesInCache), files.size(), tasks.size(),
                     computeTotalFileLength(files), execTimeNoCache, bestExecTimeCache));
               conf.setLong(OCTOPUS_PREFETCHER_MAX_WAIT, 0);
               if (dfs != null)
                  return OctopusCache.addCachePoolAndDirectives(dfs, filesInCache);
               else
                  return null;
            }
         }

         // Do not prefetch anything
         Collection<FileStatus> filesInCache = getFilesAlreadyInCache(files, filesToTasks);
         LOG.info(String.format("NOT Prefetching files=0 blocks=0 length=0 "
               + "acfiles=%d acblocks=%d aclength=%d allfiles=%d allblocks=%d alllength=%d delay=%d e1=%.0f e2=%.0f",
               filesInCache.size(), computeNumberOfBlocks(filesInCache, filesToTasks),
               computeTotalFileLength(filesInCache), files.size(), tasks.size(),
               computeTotalFileLength(files), bestCacheDelay, execTimeNoCache, bestExecTimeCache));
         return null;
      }
   }

   /**
    * Check if the file to be prefetched will offer any benefits. We consider a
    * prefetched file useful if at least on task reads some data from memory.
    * 
    * @param file
    * @param filesToTasks
    * @return
    */
   private static boolean isPrefetchedFileUseful(FileStatus file,
         Map<FileStatus, ArrayList<MRTaskModel>> filesToTasks) {
      ArrayList<MRTaskModel> tasks = filesToTasks.get(file);
      if (tasks != null) {
         for (MRTaskModel task : tasks) {
            if (task.isReadFullyFromCache() || task.isReadPartiallyFromCache()) {
               return true;
            }
         }
      }

      return false;
   }

   /**
    * Check if cache will be wasted because we are prefetching more files that can
    * be used
    * 
    * @param tasks
    * @param wasteTol
    * @param enablePartialCacheRead
    * @return
    */
   private static boolean isCacheWasted(ArrayList<MRTaskModel> tasks, double wasteTol,
         boolean enablePartialCacheRead) {

      // Compute to-be-cached length and wasted cache length
      double totalCacheLength = 0l, wastedCacheLength = 0l;
      for (MRTaskModel task : tasks) {
         if (task.hasCandidateCacheLoc()) {
            totalCacheLength += task.getLength();
            if (task.getCacheTime() <= 0) {
               // Block will not get scheduled for caching
               wastedCacheLength += task.getLength();
            } else if (!enablePartialCacheRead && task.getStartTime() < task.getCacheTime()) {
               // Task will start before caching completes
               wastedCacheLength += task.getLength();
            }
         }
      }

      if (totalCacheLength > 0 && wastedCacheLength / totalCacheLength < wasteTol)
         return false;
      else
         return true;
   }

   /**
    * Compute the sum of the file lengths
    * 
    * @param files
    * @return
    */
   private static long computeTotalFileLength(Collection<FileStatus> files) {
      long length = 0l;
      for (FileStatus file : files) {
         length += file.getLen();
      }
      return length;
   }

   /**
    * Compute the total number of blocks of the files
    * 
    * @param files
    * @param filesToTasks
    * @return
    */
   private static int computeNumberOfBlocks(Collection<FileStatus> files,
         Map<FileStatus, ArrayList<MRTaskModel>> filesToTasks) {
      int count = 0;
      for (FileStatus file : files) {
         if (filesToTasks.containsKey(file))
            count += filesToTasks.get(file).size();
      }

      return count;
   }

   /**
    * Compute the total number of blocks of the files - might not be accurate if
    * the file is not a LocatedFileStatus
    * 
    * @param files
    * @param filesToTasks
    * @return
    */
   private static int computeNumberOfBlocks(Collection<FileStatus> files) {
      int count = 0;
      for (FileStatus file : files) {
         if (file instanceof LocatedFileStatus) {
            count += ((LocatedFileStatus) file).getBlockLocations().length;
         } else {
            count += (int) Math.ceil(((double) file.getLen()) / file.getBlockSize());
         }
      }

      return count;
   }

   /**
    * Find the subset of input files that are already in the cache
    * 
    * @param files
    * @param filesToTasks
    * @return
    */
   private static Collection<FileStatus> getFilesAlreadyInCache(Collection<FileStatus> files,
         Map<FileStatus, ArrayList<MRTaskModel>> filesToTasks) {
      HashSet<FileStatus> filesInCache = new HashSet<FileStatus>();
      for (FileStatus file : files) {
         if (filesToTasks.containsKey(file) && filesToTasks.get(file).get(0).isAlreadyCached()) {
            filesInCache.add(file);
         }
      }

      return filesInCache;
   }

   /**
    * Wait for prefetching to complete. The list of cache ids and the max waiting
    * time are passed as configuration parameters.
    * 
    * @param conf
    * @return
    */
   public static boolean waitForPrefetching(Configuration conf) {

      long maxWait = conf.getLong(OCTOPUS_PREFETCHER_MAX_WAIT, 0l);
      if (maxWait <= 0)
         return false;

      HashSet<Long> ids = getCacheDirectiveIds(conf);
      if (ids == null || ids.isEmpty())
         return false;

      DistributedFileSystem dfs = getDFS(conf);
      if (dfs == null)
         return false;

      // Wait for caching to complete
      return OctopusCache.waitForCaching(dfs, ids, maxWait);
   }

   /**
    * Delete the prefetched files from the octopus cache. The deletion is either
    * immediately or after an expiration interval.
    * 
    * @param conf
    */
   public static void clearPrefetchedFiles(Configuration conf) {

      HashSet<Long> ids = getCacheDirectiveIds(conf);
      if (ids == null || ids.isEmpty())
         return;

      DistributedFileSystem dfs = getDFS(conf);
      if (dfs == null)
         return;

      long expiry = conf.getLong(OCTOPUS_PREFETCHER_EXPIRY, 0l);

      // Delete cache directives
      OctopusCache.deleteCacheDirectives(dfs, ids, expiry);
   }

   /**
    * Get the instance of the default distributed file system. May return null.
    * 
    * @param conf
    * @return
    */
   private static DistributedFileSystem getDFS(Configuration conf) {
      DistributedFileSystem dfs = null;
      try {
         FileSystem fs = FileSystem.get(conf);
         if (fs != null && fs instanceof DistributedFileSystem) {
            dfs = (DistributedFileSystem) fs;
         }
      } catch (IOException e) {
         LOG.warn(e.getClass().getSimpleName() + ": " + e.getLocalizedMessage().split("\n")[0]);
      }

      return dfs;
   }

   /**
    * Get the DFS's storage report. If there is an issue, it will return an empty
    * array.
    * 
    * @param dfs
    * @return
    */
   private static DatanodeStorageReport[] getDFSStorageReport(DistributedFileSystem dfs) {
      DatanodeStorageReport[] report = null;
      try {
         report = dfs.getClient().getDatanodeStorageReport(DatanodeReportType.LIVE);
      } catch (IOException e) {
         LOG.warn(e.getClass().getSimpleName() + ": " + e.getLocalizedMessage().split("\n")[0]);
      }

      return (report == null) ? new DatanodeStorageReport[0] : report;
   }

   /**
    * Get the cache directive IDs that were cached
    * 
    * @param conf
    * @return
    */
   private static HashSet<Long> getCacheDirectiveIds(Configuration conf) {

      String strIDs = conf.get(OCTOPUS_PREFETCHER_PREFETCHED_IDS);
      if (strIDs == null || strIDs.isEmpty())
         return null;

      // Parse the cache directive ids
      HashSet<Long> ids = new HashSet<Long>();
      try {
         strIDs = strIDs.substring(1, strIDs.length() - 1);
         StringTokenizer tokenizer = new StringTokenizer(strIDs, ",");
         while (tokenizer.hasMoreTokens()) {
            ids.add(Long.parseLong(tokenizer.nextToken().trim()));
         }
      } catch (RuntimeException e) {
         LOG.warn("Unable to parse cache directive ids string", e);
      }

      return ids;
   }

   /**
    * A comparator for sorting task models by length decreasing
    * 
    * @author hero
    */
   private static class FileStatusComparatorByLengthDecr implements Comparator<FileStatus> {

      @Override
      public int compare(FileStatus f1, FileStatus f2) {
         if (f1.getLen() < f2.getLen())
            return 1;
         else if (f1.getLen() > f2.getLen())
            return -1;
         else
            return f1.compareTo(f2);
      }

   }

   /**
    * A comparator for sorting task models by length increasing
    * 
    * @author hero
    */
   private static class FileStatusComparatorByLengthIncr implements Comparator<FileStatus> {

      @Override
      public int compare(FileStatus f1, FileStatus f2) {
         if (f1.getLen() > f2.getLen())
            return 1;
         else if (f1.getLen() < f2.getLen())
            return -1;
         else
            return f1.compareTo(f2);
      }

   }

}
