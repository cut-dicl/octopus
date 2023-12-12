package org.apache.hadoop.hdfs.util;

import java.io.IOException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveStats;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolStats;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;

public class OctopusCache {

   private static final Log LOG = LogFactory.getLog(OctopusCache.class);

   private static final String OCTOPUS_CACH_POOL = "octopus_pool";
   private static final long STEP_WAIT = 250; // in ms
   private static final long MS_IN_YEAR = 31536000000l;

   /**
    * Add octopus cache pool and cache directives for the files.
    * 
    * @param dfs
    * @param files
    * @return a collection with cache directive IDs
    */
   public static Collection<Long> addCachePoolAndDirectives(DistributedFileSystem dfs,
         Collection<FileStatus> files) {

      HashSet<Long> ids = new HashSet<Long>(files.size());
      long id;

      // Add cache pool
      if (OctopusCache.addOctopusCachePool(dfs)) {

         // Add all cache directives
         for (FileStatus file : files) {
            id = OctopusCache.addCacheDirective(dfs, file.getPath());
            if (id >= 0) {
               ids.add(id);
            }
         }

         try {
            if (ids.size() > 0)
               dfs.listCachePools().hasNext(); // This will force a rescan of the cache
         } catch (IOException e) {
            LOG.warn(e.getClass().getSimpleName() + ": " + e.getLocalizedMessage().split("\n")[0]);
         }
      }

      return ids;
   }

   /**
    * Add the default cache pool if it doesn't exist already
    * 
    * @param dfs
    * @return true if the cache pool is created or already exists
    */
   public static boolean addOctopusCachePool(DistributedFileSystem dfs) {
      boolean success = false;
      // Check if pool exist and create it
      if (!OctopusCache.octopusCachePoolExists(dfs)) {
         CachePoolInfo info = new CachePoolInfo(OctopusCache.OCTOPUS_CACH_POOL);

         long capacity = computeCacheCapacity(dfs);
         if (capacity > 0) {
            info.setLimit(capacity, StorageType.MEMORY);
         }

         try {
            dfs.addCachePool(info);
            success = true;
            LOG.info("Successfully added cache pool " + OctopusCache.OCTOPUS_CACH_POOL
                  + " with capacity " + capacity);
         } catch (IOException e) {
            LOG.warn(e.getClass().getSimpleName() + ": " + e.getLocalizedMessage().split("\n")[0]);
            success = false;
         }
      } else {
         success = true;
         LOG.info("Cache pool already exists " + OctopusCache.OCTOPUS_CACH_POOL);
      }

      return success;
   }

   /**
    * Computes and returns the total cache capacity of the file system
    * 
    * @param dfs
    * @return
    */
   private static long computeCacheCapacity(DistributedFileSystem dfs) {
      long capacity = 0l;

      try {
         DatanodeStorageReport[] report = dfs.getClient()
               .getDatanodeStorageReport(DatanodeReportType.LIVE);
         for (DatanodeStorageReport dnsr : report) {
            for (StorageReport sr : dnsr.getStorageReports()) {
               if (sr.getStorage().getStorageType() == StorageType.MEMORY) {
                  capacity += sr.getCacheCapacity();
               }
            }
         }
      } catch (IOException e) {
         LOG.warn("Unable to compute cache capacity", e);
      }

      return capacity;
   }

   /**
    * Check if the cache pool already exists
    * 
    * @param dfs
    * @return
    */
   public static boolean octopusCachePoolExists(DistributedFileSystem dfs) {
      try {
         EnumSet<CacheFlag> flags = EnumSet.noneOf(CacheFlag.class);
         flags.add(CacheFlag.SKIP_RESCAN);

         RemoteIterator<CachePoolEntry> iter = dfs.listCachePools(flags);
         while (iter.hasNext()) {
            CachePoolEntry entry = iter.next();
            CachePoolInfo info = entry.getInfo();
            if (OCTOPUS_CACH_POOL.equals(info.getPoolName()))
               return true;
         }
      } catch (IOException e) {
         LOG.warn(e.getClass().getSimpleName() + ": " + e.getLocalizedMessage().split("\n")[0]);
      }
      return false;
   }

   /**
    * Return the number of bytes remaining in the octopus cache pool
    * 
    * @param dfs
    * @return
    */
   public static long getCachePoolBytesRemaining(DistributedFileSystem dfs) {
      long remaining = 0l;

      try {
         RemoteIterator<CachePoolEntry> iter = dfs.listCachePools();
         while (iter.hasNext()) {
            CachePoolEntry entry = iter.next();
            CachePoolInfo info = entry.getInfo();
            if (OCTOPUS_CACH_POOL.equals(info.getPoolName())) {
               CachePoolStats stats = entry.getStats().get(StorageType.MEMORY);
               remaining = info.getLimit(StorageType.MEMORY) - stats.getBytesCached();
               break;
            }
         }
      } catch (IOException e) {
         LOG.warn(e.getClass().getSimpleName() + ": " + e.getLocalizedMessage().split("\n")[0]);
      }

      return remaining;
   }

   /**
    * Add the cache directive if it doesn't exist already or extend its expiration
    * date
    * 
    * @param dfs
    * @param path
    * @return id of (new or existing) cache directive or -1
    */
   public static long addCacheDirective(DistributedFileSystem dfs, Path path) {
      long id = -1;
      CacheDirectiveInfo dirInfo = getCacheDirective(dfs, path);
      EnumSet<CacheFlag> flags = EnumSet.noneOf(CacheFlag.class);
      flags.add(CacheFlag.FORCE);

      if (dirInfo == null) {
         // Directive does not exist. Create a new one
         CacheDirectiveInfo.Builder builder = new CacheDirectiveInfo.Builder();
         builder.setPath(path);
         builder.setPool(OctopusCache.OCTOPUS_CACH_POOL);
         try {
            id = dfs.addCacheDirective(builder.build(), flags);
            LOG.info("Added cache directive " + id + " for file " + path.toUri().getPath());
         } catch (IOException e) {
            LOG.warn(e.getClass().getSimpleName() + ": " + e.getLocalizedMessage().split("\n")[0]);
         }
      } else {
         if (dirInfo.getExpiration().getAbsoluteMillis() > System.currentTimeMillis()
               + MS_IN_YEAR) {
            id = dirInfo.getId();
            LOG.info("Retain cache directive for file " + path.toUri().getPath());
         } else {
            // Change the expiration time to "never"
            CacheDirectiveInfo.Builder builder = new CacheDirectiveInfo.Builder(dirInfo);
            builder.setExpiration(CacheDirectiveInfo.Expiration.NEVER);
            try {
               dfs.modifyCacheDirective(builder.build(), flags);
               id = dirInfo.getId();
               LOG.info(
                     "Modified cache directive to never expire for file " + path.toUri().getPath());
            } catch (IOException e) {
               LOG.warn(
                     e.getClass().getSimpleName() + ": " + e.getLocalizedMessage().split("\n")[0]);
            }
         }
      }

      return id;
   }

   /**
    * Check if a cache directive exists for the provided path
    * 
    * @param dfs
    * @param path
    * @return
    */
   public static boolean cacheDirectiveExists(DistributedFileSystem dfs, Path path) {
      CacheDirectiveInfo.Builder builder = new CacheDirectiveInfo.Builder();
      builder.setPath(path);
      builder.setPool(OCTOPUS_CACH_POOL);

      EnumSet<CacheFlag> flags = EnumSet.noneOf(CacheFlag.class);
      flags.add(CacheFlag.SKIP_RESCAN);

      try {
         RemoteIterator<CacheDirectiveEntry> iter = dfs.listCacheDirectives(builder.build(), flags);
         if (iter.hasNext())
            return true;
      } catch (IOException e) {
         LOG.warn(e.getClass().getSimpleName() + ": " + e.getLocalizedMessage().split("\n")[0]);
      }
      return false;
   }

   /**
    * Delete the cache directive associated with this id, if exists.
    * 
    * @param dfs
    * @param id
    * @return
    */
   public static boolean deleteCacheDirective(DistributedFileSystem dfs, Long id) {

      // Check if the path exist in default pool and remove it
      CacheDirectiveInfo.Builder builder = new CacheDirectiveInfo.Builder();
      builder.setId(id);
      builder.setPool(OctopusCache.OCTOPUS_CACH_POOL);

      EnumSet<CacheFlag> flags = EnumSet.noneOf(CacheFlag.class);
      flags.add(CacheFlag.SKIP_RESCAN);

      try {
         RemoteIterator<CacheDirectiveEntry> iter = dfs.listCacheDirectives(builder.build(), flags);
         if (iter.hasNext()) {
            CacheDirectiveEntry entry = iter.next();
            dfs.removeCacheDirective(entry.getInfo().getId());
            LOG.info("Deleted cache directive with id " + id);
            return true;
         }
      } catch (IOException e) {
         LOG.warn(e.getClass().getSimpleName() + ": " + e.getLocalizedMessage().split("\n")[0]);
      }

      return false;
   }

   /**
    * Delete the cache directive by setting the relative expiry time in
    * milliseconds. If expiry is less than 0, the directive is deleted immediately.
    * 
    * @param dfs
    * @param id
    * @param expiry_ms
    * @return
    */
   public static boolean deleteCacheDirective(DistributedFileSystem dfs, Long id, long expiry_ms) {
      if (expiry_ms <= 0)
         return deleteCacheDirective(dfs, id);

      // Get current directive
      CacheDirectiveInfo dirInfo = getCacheDirective(dfs, id);
      if (dirInfo == null)
         return true;

      // Update the expiration of the current directive
      CacheDirectiveInfo.Builder builder = new CacheDirectiveInfo.Builder(dirInfo);
      builder.setExpiration(CacheDirectiveInfo.Expiration.newRelative(expiry_ms));
      EnumSet<CacheFlag> flags = EnumSet.noneOf(CacheFlag.class);
      flags.add(CacheFlag.FORCE);
      boolean success = false;

      try {
         dfs.modifyCacheDirective(builder.build(), flags);
         success = true;
         LOG.info("Modified cache directive with id " + id + " to expire in " + expiry_ms + "ms");
      } catch (IOException e) {
         LOG.warn(e.getClass().getSimpleName() + ": " + e.getLocalizedMessage().split("\n")[0]);
      }

      return success;
   }

   /**
    * Delete all the cache directives by setting the relative expiry time in
    * milliseconds. If expiry is less than 0, the directives are deleted
    * immediately.
    * 
    * @param dfs
    * @param ids
    * @param expiry_ms
    * @return
    */
   public static void deleteCacheDirectives(DistributedFileSystem dfs, Set<Long> ids,
         long expiry_ms) {
      for (Long id : ids) {
         deleteCacheDirective(dfs, id, expiry_ms);
      }

      try {
         if (ids.size() > 0)
            dfs.listCachePools().hasNext(); // This will force a rescan of the cache
      } catch (IOException e) {
         LOG.warn(e.getClass().getSimpleName() + ": " + e.getLocalizedMessage().split("\n")[0]);
      }
   }

   /**
    * Get the current cache directive. Returns null if not found.
    * 
    * @param dfs
    * @param id
    * @return
    */
   private static CacheDirectiveInfo getCacheDirective(DistributedFileSystem dfs, Long id) {
      CacheDirectiveInfo.Builder builder = new CacheDirectiveInfo.Builder();
      builder.setId(id);
      builder.setPool(OCTOPUS_CACH_POOL);

      EnumSet<CacheFlag> flags = EnumSet.noneOf(CacheFlag.class);
      flags.add(CacheFlag.SKIP_RESCAN);

      try {
         RemoteIterator<CacheDirectiveEntry> iter = dfs.listCacheDirectives(builder.build(), flags);
         if (iter.hasNext()) {
            return iter.next().getInfo();
         }
      } catch (IOException e) {
         LOG.warn(e.getClass().getSimpleName() + ": " + e.getLocalizedMessage().split("\n")[0]);
      }

      return null;
   }

   /**
    * Get the current cache directive. Returns null if not found.
    * 
    * @param dfs
    * @param pth
    * @return
    */
   private static CacheDirectiveInfo getCacheDirective(DistributedFileSystem dfs, Path path) {
      CacheDirectiveInfo.Builder builder = new CacheDirectiveInfo.Builder();
      builder.setPath(path);
      builder.setPool(OCTOPUS_CACH_POOL);

      EnumSet<CacheFlag> flags = EnumSet.noneOf(CacheFlag.class);
      flags.add(CacheFlag.SKIP_RESCAN);

      try {
         RemoteIterator<CacheDirectiveEntry> iter = dfs.listCacheDirectives(builder.build(), flags);
         if (iter.hasNext()) {
            return iter.next().getInfo();
         }
      } catch (IOException e) {
         LOG.warn(e.getClass().getSimpleName() + ": " + e.getLocalizedMessage().split("\n")[0]);
      }

      return null;
   }

   /**
    * Wait until the cache directives with the provided ids get cached or until
    * maxWait.
    * 
    * @param dfs
    * @param ids
    * @param maxWait_ms
    * @return true if we know that the files have just been cached
    */
   public static boolean waitForCaching(DistributedFileSystem dfs, Set<Long> ids, long maxWait_ms) {
      if (maxWait_ms <= 0 || ids.isEmpty())
         return false; // no wait

      CacheDirectiveInfo.Builder builder = new CacheDirectiveInfo.Builder();
      builder.setPool(OCTOPUS_CACH_POOL);
      CacheDirectiveInfo filter = builder.build();
      long totalBytesLeft = 1l;
      int count = 0;

      while (totalBytesLeft > 0 && (count * STEP_WAIT < maxWait_ms)) {
         try {
            Thread.sleep(Math.min(STEP_WAIT, maxWait_ms - (count * STEP_WAIT)));
            ++count;

            // Sum up total bytes needed for caching
            totalBytesLeft = 0l;
            RemoteIterator<CacheDirectiveEntry> iter = dfs.listCacheDirectives(filter);
            while (iter.hasNext()) {
               CacheDirectiveEntry entry = iter.next();
               if (ids.contains(entry.getInfo().getId())) {
                  CacheDirectiveStats stats = entry.getStats().get(StorageType.MEMORY);
                  totalBytesLeft += (stats.getBytesNeeded() - stats.getBytesCached());
               }
            }
         } catch (IOException e) {
            LOG.warn(e.getClass().getSimpleName() + ": " + e.getLocalizedMessage().split("\n")[0]);
            break;
         } catch (InterruptedException e) {
            LOG.warn(e.getClass().getSimpleName() + ": " + e.getLocalizedMessage().split("\n")[0]);
            break;
         }
      }

      LOG.info(
            "Waited for caching " + (count * STEP_WAIT) + " ms and bytes left = " + totalBytesLeft);
      return totalBytesLeft == 0;
   }
}
