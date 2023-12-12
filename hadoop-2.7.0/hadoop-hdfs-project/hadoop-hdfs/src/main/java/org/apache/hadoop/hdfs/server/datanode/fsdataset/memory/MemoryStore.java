package org.apache.hadoop.hdfs.server.datanode.fsdataset.memory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;

/**
 * Stores block data and metadata in memory. It categorizes the blocks based on
 * their BPID. Keeps track of all the usage tracks.
 */
public class MemoryStore {

   static final Log LOG = LogFactory.getLog(MemoryStore.class);

   // BPID => { BlockId => MemoryBlock }
   private final Map<String, Map<Long, MemoryBlock>> blocksMap;

   private final ByteBufferPool dataPool;
   private final ByteBufferPool metaPool;

   private final long capacity; // total memory capacity
   private AtomicLong reserved; // reserved bytes for RBW
   private MemoryStats totalStats; // total usage stats
   private Map<String, MemoryStats> bpStats; // usage stats per bpool

   //Added from Elena for caching
   private long cacheCapacity;
   private MemoryStats totalCacheStats;
   private Map<String, MemoryStats> bpCacheStats;
  
   
   /**
    * Constructor
    * 
    * @param conf
    */
   MemoryStore(Configuration conf) {
      this.blocksMap = new ConcurrentHashMap<String, Map<Long, MemoryBlock>>();
      this.dataPool = new ByteBufferPool(1024 * conf.getInt(
            DFSConfigKeys.DFS_DATANODE_MEMORY_DATAPOOL_KEY,
            DFSConfigKeys.DFS_DATANODE_MEMORY_DATAPOOL_DEFAULT));
      this.metaPool = new ByteBufferPool(1024 * conf.getInt(
            DFSConfigKeys.DFS_DATANODE_MEMORY_METAPOOL_KEY,
            DFSConfigKeys.DFS_DATANODE_MEMORY_METAPOOL_DEFAULT));
      this.capacity = conf.getLong(
            DFSConfigKeys.DFS_DATANODE_MEMORY_CAPACITY_KEY,
            DFSConfigKeys.DFS_DATANODE_MEMORY_CAPACITY_DEFAULT) * 1024 * 1024;
      this.reserved = new AtomicLong(0L);
      this.totalStats = new MemoryStats();
      this.bpStats = new HashMap<String, MemoryStats>();
      
      //Added from Elena - initialize cache capacity
      float cachePerc = conf.getFloat(
              DFSConfigKeys.DFS_DATANODE_CACHE_CAPACITY_PERCENT_PARTIAL_KEY
                    + StorageType.MEMORY.toString().toLowerCase(),
              DFSConfigKeys.DFS_DATANODE_CACHE_CAPACITY_PERCENT_DEFAULT_MEMORY);
      cachePerc = (cachePerc < 0) ? 0 : cachePerc;
      cachePerc = (cachePerc > 1) ? 1 : cachePerc;
      this.cacheCapacity = (long)(cachePerc * getCapacity());
      this.totalCacheStats =  new MemoryStats();
      this.bpCacheStats = new HashMap<String, MemoryStats>();
   }

   /**
    * Add (or replace) a memory block in the store
    * 
    * @param bpid
    * @param b
    */
   void addMemoryBlock(String bpid, MemoryBlock b) {
      checkBlockPool(bpid);
      checkBlock(b);
      synchronized (this) {
         Map<Long, MemoryBlock> bpBlocks = blocksMap.get(bpid);
         if (bpBlocks == null) {
            // Add an entry for block pool if it does not exist already
            bpBlocks = new HashMap<Long, MemoryBlock>();
            blocksMap.put(bpid, bpBlocks);
            bpStats.put(bpid, new MemoryStats(totalStats));
            bpCacheStats.put(bpid, new MemoryStats(totalCacheStats));
         }

         bpBlocks.put(b.getBlockId(), b);
      }
   }

   /**
    * Get the memory block from the store. Returns null if not found. Also
    * checks the generation stamp.
    * 
    * @param bpid
    * @param b
    * @return
    */
   MemoryBlock getMemoryBlock(String bpid, Block b) {
      checkBlockPool(bpid);
      checkBlock(b);
      synchronized (this) {
         Map<Long, MemoryBlock> map = blocksMap.get(bpid);
         if (map == null)
            return null;

         MemoryBlock memBlock = map.get(b.getBlockId());
         if (memBlock != null
               && b.getGenerationStamp() == memBlock.getGenerationStamp()) {
            return memBlock;
         } else
            return null;
      }
   }

   /**
    * Get the memory block from the store. Returns null if not found.
    * 
    * @param bpid
    * @param b
    * @return
    */
   MemoryBlock getMemoryBlock(String bpid, long blockId) {
      checkBlockPool(bpid);
      synchronized (this) {
         Map<Long, MemoryBlock> map = blocksMap.get(bpid);
         return map != null ? map.get(blockId) : null;
      }
   }

   /**
    * Remove a memory block from the store. This method also deletes the actual
    * data & meta
    * 
    * @param bpid
    * @param b
    * @return
    */
   MemoryBlock removeMemoryBlock(String bpid, Block b) {
      checkBlockPool(bpid);
      checkBlock(b);
      synchronized (this) {
         Map<Long, MemoryBlock> map = blocksMap.get(bpid);
         if (map != null) {
            MemoryBlock blockToDel = map.get(b.getBlockId());

            if (blockToDel != null
                  && b.getGenerationStamp() == blockToDel.getGenerationStamp()) {
               // Also delete all data and metadata
               blockToDel.getData().delete(dataPool);
               blockToDel.getMeta().delete(metaPool);

               return map.remove(b.getBlockId());
            }
         }
      }

      return null;
   }

   boolean hasMemoryBlock(String bpid, Block b) {
      checkBlockPool(bpid);
      checkBlock(b);
      synchronized (this) {
         if (blocksMap.containsKey(bpid)) {
            return blocksMap.get(bpid).containsKey(b.getBlockId());
         }
         return false;
      }

   }

   boolean hasMemoryBlock(String bpid, long blockId) {
      checkBlockPool(bpid);
      synchronized (this) {
         if (blocksMap.containsKey(bpid)) {
            return blocksMap.get(bpid).containsKey(blockId);
         }
         return false;
      }
   }

   ByteBufferPool getDataPool() {
      return dataPool;
   }

   ByteBufferPool getMetaPool() {
      return metaPool;
   }

   long getCapacity() {
      return capacity;
   }

   /**
    * @return the used space for data and metadata
    */
   synchronized long getDfsUsed() {
      return totalStats.getReportedUsage() + totalCacheStats.getReportedUsage();
   }

   synchronized void reserveSpaceForRbw(long bytesToReserve) {
      if (bytesToReserve != 0) {
         reserved.addAndGet(bytesToReserve);
      }
   }

   /**
    * @return capacity minus usage minus reserved
    */
   synchronized long getAvailable() {
      long remaining = capacity - totalStats.getActualUsage() - reserved.get() - totalCacheStats.getActualUsage();
      return (remaining > 0) ? remaining : 0;
   }

   synchronized long getReserved() {
      return reserved.get();
   }

   synchronized void releaseReservedSpace(long bytesToRelease) {
      if (bytesToRelease != 0) {
         long oldReservation, newReservation;
         do {
            oldReservation = reserved.get();
            newReservation = oldReservation - bytesToRelease;
            if (newReservation < 0) {
               // Failsafe, this should never occur in practice
               newReservation = 0;
            }
         } while (!reserved.compareAndSet(oldReservation, newReservation));
      }
   }

   synchronized String[] getBlockPoolList() {
      return blocksMap.keySet().toArray(new String[blocksMap.keySet().size()]);
   }

   /**
    * @param bpid
    * @return the used space for data and metadata for this pool
    */
   long getBlockPoolUsed(String bpid) throws IOException {
      checkBlockPool(bpid);
      synchronized (this) {
         if (bpStats.containsKey(bpid) && bpCacheStats.containsKey(bpid))
            return bpStats.get(bpid).getReportedUsage() + bpCacheStats.get(bpid).getReportedUsage();
         else
            throw new IOException("Block pool id " + bpid + " not found.");
      }
   }

   synchronized void shutdown() {
      List<String> allBpids = new ArrayList<>(blocksMap.keySet());

      for (String bpid : allBpids) {
         shutdownBlockPool(bpid);
      }

      // Clear pools
      dataPool.clear();
      metaPool.clear();
   }

   void addBlockPool(String bpid) {
      checkBlockPool(bpid);
      synchronized (this) {
         if (!blocksMap.containsKey(bpid)) {
            blocksMap.put(bpid, new HashMap<Long, MemoryBlock>());
            bpStats.put(bpid, new MemoryStats(totalStats));
            bpCacheStats.put(bpid, new MemoryStats(totalCacheStats));
         }
      }
   }

   /**
    * Remove the block pool and ALL the block data and metadata
    * 
    * @param bpid
    */
   void shutdownBlockPool(String bpid) {
      checkBlockPool(bpid);
      synchronized (this) {
         if (blocksMap.containsKey(bpid)) {
            // Delete all memory blocks
            Map<Long, MemoryBlock> bpMap = blocksMap.get(bpid);
            for (MemoryBlock block : bpMap.values()) {
               block.getData().delete(dataPool);
               block.getMeta().delete(metaPool);
            }

            // Clear all maps
            bpMap.clear();
            blocksMap.remove(bpid);
            bpStats.remove(bpid);
            bpCacheStats.remove(bpid);
         }
      }
   }

   BlockListAsLongs getBlockReport(String bpid) {
      BlockListAsLongs.Builder builder = BlockListAsLongs.builder();

      synchronized (this) {
         if (blocksMap.get(bpid) != null) {
            for (MemoryBlock block : blocksMap.get(bpid).values()) {
               switch (block.getState()) {
               case FINALIZED:
               case RBW:
               case RWR:
                  builder.add(block);
                  break;
               case RUR:
                  MemoryBlockUnderRecovery rur = (MemoryBlockUnderRecovery) block;
                  builder.add(rur.getOriginalReplica());
                  break;
               case TEMPORARY:
                  break;
               case CACHING:
                   break;
               case CACHED:
                   break;    
               default:
                  assert false : "Illegal ReplicaState.";
               }
            }
         }
      }

      return builder.build();
   }

   MemoryStats getBpMemoryStats(String bpid) throws IOException {
      checkBlockPool(bpid);
      synchronized (this) {
         MemoryStats blockPoolMemoryStats = bpStats.get(bpid);

         if (blockPoolMemoryStats != null)
            return blockPoolMemoryStats;
         else
            throw new IOException("Block pool id " + bpid + " not found.");
      }
   }

   private void checkBlockPool(String bpid) {
      if (bpid == null) {
         throw new IllegalArgumentException("Block Pool Id is null");
      }
   }

   private void checkBlock(Block b) {
      if (b == null) {
         throw new IllegalArgumentException("Block is null");
      }
   }

   //--------------------------
   //Methods added for caching
   //--------------------------
   
   long getCacheCapacity() {
       return cacheCapacity;
    }

    /**
     * @return the cache used space
     */
    synchronized long getCacheUsed() {
       return totalCacheStats.getReportedUsage();
    }
   
    /**
     * @return cache available capacity 
     */
    synchronized long getCacheAvailable() {
        long remaining = Math.min(cacheCapacity - totalCacheStats.getActualUsage(), getAvailable());   
        return (remaining > 0) ? remaining : 0;
    }
   
   MemoryStats getBpCacheStats(String bpid) throws IOException {
       checkBlockPool(bpid);
       synchronized (this) {
          MemoryStats blockPoolCacheStats = bpCacheStats.get(bpid);

          if (blockPoolCacheStats != null)
             return blockPoolCacheStats;
          else
             throw new IOException("Block pool cache id " + bpid + " not found.");
       }
    }
   
   long getBpCacheUsed(String bpid) throws IOException {
       checkBlockPool(bpid);
       synchronized (this) {
           if (bpCacheStats.containsKey(bpid))
               return bpCacheStats.get(bpid).getReportedUsage();
           else
               throw new IOException("Block pool cache id " + bpid + " not found.");
       }
   }

   List<Long> getCacheReport(String bpid){
       List<Long> cacheReport = new ArrayList<Long>(blocksMap.size());
       
       synchronized (this) {
           if (blocksMap.get(bpid) != null) {
              for (MemoryBlock block : blocksMap.get(bpid).values()) {
                 switch (block.getState()) {
                 case FINALIZED:
                 case RBW:
                 case RWR:              
                 case RUR: 
                 case TEMPORARY:              
                 case CACHING:
                     break;
                 case CACHED:
                     cacheReport.add(block.getBlockId());
                     break;    
                 default:
                    assert false : "Illegal ReplicaState.";
                 }
              }
           }
        }
       
       return cacheReport;
   }
   
}
