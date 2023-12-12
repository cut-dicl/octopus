/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.blockmanagement;

import static org.apache.hadoop.util.ExitUtil.terminate;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.ReplicationVector;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.CacheDirective;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo.CachedBlocksList.Type;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor.DatanodeDescrTypePairs;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.namenode.CacheManager;
import org.apache.hadoop.hdfs.server.namenode.CachePool;
import org.apache.hadoop.hdfs.server.namenode.CachedBlock;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.apache.hadoop.util.GSet;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
;

/**
 * Scans the namesystem, scheduling blocks to be cached as appropriate.
 *
 * The CacheReplicationMonitor does a full scan when the NameNode first
 * starts up, and at configurable intervals afterwards.
 */
@InterfaceAudience.LimitedPrivate({"HDFS"})
public class CacheReplicationMonitor extends Thread implements Closeable {

  private static final Logger LOG =
      LoggerFactory.getLogger(CacheReplicationMonitor.class);

  private final FSNamesystem namesystem;

  private final BlockManager blockManager;

  private final CacheManager cacheManager;

  private final GSet<CachedBlock, CachedBlock> cachedBlocks;

  /**
   * Pseudorandom number source
   */
  private static final Random random = new Random();

  /**
   * The interval at which we scan the namesystem for caching changes.
   */
  private final long intervalMs;

  /**
   * The CacheReplicationMonitor (CRM) lock. Used to synchronize starting and
   * waiting for rescan operations.
   */
  private final ReentrantLock lock;

  /**
   * Notifies the scan thread that an immediate rescan is needed.
   */
  private final Condition doRescan;

  /**
   * Notifies waiting threads that a rescan has finished.
   */
  private final Condition scanFinished;

  /**
   * The number of rescans completed. Used to wait for scans to finish.
   * Protected by the CacheReplicationMonitor lock.
   */
  private long completedScanCount = 0;

  /**
   * The scan we're currently performing, or -1 if no scan is in progress.
   * Protected by the CacheReplicationMonitor lock.
   */
  private long curScanCount = -1;

  /**
   * The number of rescans we need to complete.  Protected by the CRM lock.
   */
  private long neededScanCount = 0;

  /**
   * True if this monitor should terminate. Protected by the CRM lock.
   */
  private boolean shutdown = false;

  /**
   * Mark status of the current scan.
   */
  private byte mark = 0;

  /**
   * Cache directives found in the previous scan.
   */
  private int scannedDirectives;

  /**
   * Blocks found in the previous scan.
   */
  private long scannedBlocks;

  public CacheReplicationMonitor(FSNamesystem namesystem,
      CacheManager cacheManager, long intervalMs, ReentrantLock lock) {
    this.namesystem = namesystem;
    this.blockManager = namesystem.getBlockManager();
    this.cacheManager = cacheManager;
    this.cachedBlocks = cacheManager.getCachedBlocks();
    this.intervalMs = intervalMs;
    this.lock = lock;
    this.doRescan = this.lock.newCondition();
    this.scanFinished = this.lock.newCondition();
  }

  @Override
  public void run() {
    long startTimeMs = 0;
    Thread.currentThread().setName("CacheReplicationMonitor(" +
        System.identityHashCode(this) + ")");
    LOG.info("Starting CacheReplicationMonitor with interval " +
             intervalMs + " milliseconds");
    try {
      boolean rescanFromPending = false;
      long curTimeMs = Time.monotonicNow();
      while (true) {
        lock.lock();
        try {
          while (true) {
            if (shutdown) {
              LOG.debug("Shutting down CacheReplicationMonitor");
              return;
            }
            if (completedScanCount < neededScanCount) {
              rescanFromPending = true;
              LOG.debug("Rescanning because of pending operations");
              break;
            }
            long delta = (startTimeMs + intervalMs) - curTimeMs;
            if (delta <= 0) {
              LOG.debug("Rescanning after {} milliseconds", (curTimeMs - startTimeMs));
              break;
            }
            doRescan.await(delta, TimeUnit.MILLISECONDS);
            curTimeMs = Time.monotonicNow();
          }
        } finally {
          lock.unlock();
        }
        startTimeMs = curTimeMs;
        mark = (byte) ~mark; //wrong!!!!
        rescan();
        curTimeMs = Time.monotonicNow();
        // Update synchronization-related variables.
        lock.lock();
        try {
          completedScanCount = curScanCount;
          curScanCount = -1;
          scanFinished.signalAll();
        } finally {
          lock.unlock();
        }
        if (rescanFromPending)
          LOG.info("Scanned {} directive(s) and {} block(s) in {} millisecond(s).",
            scannedDirectives, scannedBlocks, (curTimeMs - startTimeMs));
        rescanFromPending = false;
      }
    } catch (InterruptedException e) {
      LOG.info("Shutting down CacheReplicationMonitor.");
      return;
    } catch (Throwable t) {
      LOG.error("Thread exiting", t);
      terminate(1, t);
    }
  }

  /**
   * Waits for a rescan to complete. This doesn't guarantee consistency with
   * pending operations, only relative recency, since it will not force a new
   * rescan if a rescan is already underway.
   * <p>
   * Note that this call will release the FSN lock, so operations before and
   * after are not atomic.
   */
  public void waitForRescanIfNeeded() {
    Preconditions.checkArgument(!namesystem.hasWriteLock(),
        "Must not hold the FSN write lock when waiting for a rescan.");
    Preconditions.checkArgument(lock.isHeldByCurrentThread(),
        "Must hold the CRM lock when waiting for a rescan.");
    if (neededScanCount <= completedScanCount) {
      return;
    }
    // If no scan is already ongoing, mark the CRM as dirty and kick
    if (curScanCount < 0) {
      doRescan.signal();
    }
    // Wait until the scan finishes and the count advances
    while ((!shutdown) && (completedScanCount < neededScanCount)) {
      try {
        scanFinished.await();
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while waiting for CacheReplicationMonitor"
            + " rescan", e);
        break;
      }
    }
  }

  /**
   * Indicates to the CacheReplicationMonitor that there have been CacheManager
   * changes that require a rescan.
   */
  public void setNeedsRescan() {
    Preconditions.checkArgument(lock.isHeldByCurrentThread(),
        "Must hold the CRM lock when setting the needsRescan bit.");
    if (curScanCount >= 0) {
      // If there is a scan in progress, we need to wait for the scan after
      // that.
      neededScanCount = curScanCount + 1;
    } else {
      // If there is no scan in progress, we need to wait for the next scan.
      neededScanCount = completedScanCount + 1;
    }
  }

  /**
   * Shut down the monitor thread.
   */
  @Override
  public void close() throws IOException {
    Preconditions.checkArgument(namesystem.hasWriteLock());
    lock.lock();
    try {
      if (shutdown) return;
      // Since we hold both the FSN write lock and the CRM lock here,
      // we know that the CRM thread cannot be currently modifying
      // the cache manager state while we're closing it.
      // Since the CRM thread checks the value of 'shutdown' after waiting
      // for a lock, we know that the thread will not modify the cache
      // manager state after this point.
      shutdown = true;
      doRescan.signalAll();
      scanFinished.signalAll();
    } finally {
      lock.unlock();
    }
  }

  private void rescan() throws InterruptedException {
    scannedDirectives = 0;
    scannedBlocks = 0;
    try {
      namesystem.writeLock();
      try {
        lock.lock();
        if (shutdown) {
          throw new InterruptedException("CacheReplicationMonitor was " +
              "shut down.");
        }
        curScanCount = completedScanCount + 1;
      } finally {
        lock.unlock();
      }

      resetStatistics();
      rescanCacheDirectives();
      rescanCachedBlockMap();
      blockManager.getDatanodeManager().resetLastCachingDirectiveSentTime();
    } finally {
      namesystem.writeUnlock();
    }
  }

  private void resetStatistics() {
      for (CachePool pool: cacheManager.getCachePools()) {
          pool.resetStatistics();
      }
      for (CacheDirective directive: cacheManager.getCacheDirectives()) {
          directive.resetStatistics(); 
      }
  }

  /**
   * Scan all CacheDirectives.  Use the information to figure out
   * what cache replication factor each block should have.
   */
  private void rescanCacheDirectives() {
    FSDirectory fsDir = namesystem.getFSDirectory();
    final long now = new Date().getTime();
    for (CacheDirective directive : cacheManager.getCacheDirectives()) {
      scannedDirectives++;
      // Skip processing this entry if it has expired
      if (directive.getExpiryTime() > 0 && directive.getExpiryTime() <= now) {
        LOG.debug("Directive {}: the directive expired at {} (now = {})",
             directive.getId(), directive.getExpiryTime(), now);
        continue;
      }
      String path = directive.getPath();
      INode node;
      try {
        node = fsDir.getINode(path);
      } catch (UnresolvedLinkException e) {
        // We don't cache through symlinks
        LOG.debug("Directive {}: got UnresolvedLinkException while resolving "
                + "path {}", directive.getId(), path
        );
        continue;
      }
      if (node == null)  {
        LOG.debug("Directive {}: No inode found at {}", directive.getId(),
            path);
      } else if (node.isDirectory()) {
        INodeDirectory dir = node.asDirectory();
        ReadOnlyList<INode> children = dir
            .getChildrenList(Snapshot.CURRENT_STATE_ID);
        for (INode child : children) {
          if (child.isFile()) {
            rescanFile(directive, child.asFile());
          }
        }
      } else if (node.isFile()) {
        rescanFile(directive, node.asFile());
      } else {
        LOG.debug("Directive {}: ignoring non-directive, non-file inode {} ",
            directive.getId(), node);
      }
    }
  }
  
  /**
   * Apply a CacheDirective to a file.
   * 
   * @param directive The CacheDirective to apply.
   * @param file The file.
   */
  private void rescanFile(CacheDirective directive, INodeFile file) {
   
    ReplicationVector rv = new ReplicationVector(directive.getReplVector());
    for (StorageType type : StorageType.values()) {
        short replication = rv.getReplication(type);
        if (replication > 0)
            rescanFile(directive, file, type, replication);
    }
    
  }
  
  //Changed from Elena
  private void rescanFile(CacheDirective directive, INodeFile file, 
          StorageType type, short replication) {        
    BlockInfoContiguous[] blockInfos = file.getBlocks();

    // Increment the "needed" statistics
    directive.addFilesNeeded(1, type); 
    // We don't cache UC blocks, don't add them to the total here
    long neededTotal = file.computeFileSizeNotIncludingLastUcBlock() * replication;
    directive.addBytesNeeded(neededTotal, type); 

    // The pool's bytesNeeded is incremented as we scan. If the demand
    // thus far plus the demand of this file would exceed the pool's limit,
    // do not cache this file.
    CachePool pool = directive.getPool();

    if (pool.getLimit(type) == null) {
       LOG.debug("Directive {}: not scanning file {} because " +
           "caching on {} on pool {} is not enabled",
           directive.getId(),
           file.getFullPathName(),
           type.toString(),
           pool.getPoolName());
       return;
     }

    if (pool.getBytesNeeded(type) > pool.getLimit(type)) {
      LOG.debug("Directive {}: not scanning file {} because " +
          "bytesNeeded for pool {} is {}, but the pool's limit is {}",
          directive.getId(),
          file.getFullPathName(),
          pool.getPoolName(),
          pool.getBytesNeeded(type),
          pool.getLimit(type));
      return;
    }

    long cachedTotal = 0;
    for (BlockInfoContiguous blockInfo : blockInfos) {
      if (!blockInfo.getBlockUCState().equals(BlockUCState.COMPLETE)) {
        // We don't try to cache blocks that are under construction.
        LOG.trace("Directive {}: can't cache block {} because it is in state "
                + "{}, not COMPLETE.", directive.getId(), blockInfo,
            blockInfo.getBlockUCState()
        );
        continue;
      }
      Block block = new Block(blockInfo.getBlockId());
      CachedBlock ncblock = new CachedBlock(block.getBlockId(),
          directive.getReplVector(), mark);
      CachedBlock ocblock = cachedBlocks.get(ncblock);
      if (ocblock == null) {
        cachedBlocks.put(ncblock);
        ocblock = ncblock;
      } else {
        // Update bytesUsed using the current replication levels.
        // Assumptions: we assume that all the blocks are the same length
        // on each datanode.  We can assume this because we're only caching
        // blocks in state COMPLETE.
        // Note that if two directives are caching the same block(s), they will
        // both get them added to their bytesCached.
        List<DatanodeStorageInfo> cachedOn = ocblock.getDatanodes(Type.CACHED);
        
        ReplicationVector cachedOnRV = computeRV(cachedOn);
        long cachedByBlock = Math.min(cachedOnRV.getReplication(type), replication) 
                                * blockInfo.getNumBytes();
        cachedTotal += cachedByBlock;

        // Overwrite the block's replication and mark in two cases:
        if (mark != ocblock.getMark()) { 
          // 1. If the mark on the CachedBlock is different from the mark for
          // this scan, that means the block hasn't been updated during this
          // scan, and we should overwrite whatever is there, since it is no
          // longer valid.
          ocblock.setReplVectorAndMark(directive.getReplVector(), mark); 
        }
        
        if (ocblock.getReplication(type) < replication) { 
            // 2. If the replication in the CachedBlock is less than what the
            // directive asks for, we want to increase the block's replication
            // field to what the directive asks for.
            ocblock.setReplVectorAndMark(type, replication, mark); 
        }
      }
      LOG.trace("Directive {}: setting replication for block {} to {}",
          directive.getId(), blockInfo, ocblock.getReplVector());
    }
    // Increment the "cached" statistics
    directive.addBytesCached(cachedTotal, type);
    if (cachedTotal == neededTotal) {
      directive.addFilesCached(1, type);        
    }
    LOG.debug("Directive {}: caching {}: {}/{} bytes", directive.getId(),
        file.getFullPathName(), cachedTotal, neededTotal);
  }

  private String findReasonForNotCaching(CachedBlock cblock, 
          BlockInfoContiguous blockInfo) {
    if (blockInfo == null) {
      // Somehow, a cache report with the block arrived, but the block
      // reports from the DataNode haven't (yet?) described such a block.
      // Alternately, the NameNode might have invalidated the block, but the
      // DataNode hasn't caught up.  In any case, we want to tell the DN
      // to uncache this.
      return "not tracked by the BlockManager";
    } else if (!blockInfo.isComplete()) {
      // When a cached block changes state from complete to some other state
      // on the DataNode (perhaps because of append), it will begin the
      // uncaching process.  However, the uncaching process is not
      // instantaneous, especially if clients have pinned the block.  So
      // there may be a period of time when incomplete blocks remain cached
      // on the DataNodes.
      return "not complete";
    } else if (cblock.getReplVector() == 0) {
      // Since 0 is not a valid value for a cache directive's replication
      // field, seeing a replication of 0 on a CacheBlock means that it
      // has never been reached by any sweep.
      return "not needed by any directives";
    } else if (cblock.getMark() != mark) { 
      // Although the block was needed in the past, we didn't reach it during
      // the current sweep.  Therefore, it doesn't need to be cached any more.
      // Need to set the replication to 0 so it doesn't flip back to cached
      // when the mark flips on the next scan
      cblock.setReplVectorAndMark((long)0, mark);
      return "no longer needed by any directives";
    }
    return null;
  }

  /**
   * Scan through the cached block map.
   * Any blocks which are under-replicated should be assigned new Datanodes.
   * Blocks that are over-replicated should be removed from Datanodes.
   */
  private void rescanCachedBlockMap() {
    for (Iterator<CachedBlock> cbIter = cachedBlocks.iterator();
        cbIter.hasNext(); ) {
      scannedBlocks++;
      CachedBlock cblock = cbIter.next();
      List<DatanodeStorageInfo> pendingCached =
          cblock.getDatanodes(Type.PENDING_CACHED);
      List<DatanodeStorageInfo> cached =
          cblock.getDatanodes(Type.CACHED);
      List<DatanodeStorageInfo> pendingUncached =
          cblock.getDatanodes(Type.PENDING_UNCACHED);
      // Remove nodes from PENDING_UNCACHED if they were actually uncached.
      for (Iterator<DatanodeStorageInfo> iter = pendingUncached.iterator();
          iter.hasNext(); ) {
          DatanodeStorageInfo datanode = iter.next();
        if (!cblock.isInList(datanode.getCached())) {
          LOG.trace("Block {}: removing from PENDING_UNCACHED for node {} "
              + "because the DataNode uncached it.", cblock.getBlockId(),
              datanode.getDatanodeDescriptor().getDatanodeUuid());
          datanode.getPendingUncached().remove(cblock);
          iter.remove();
        }
      }
      BlockInfoContiguous blockInfo = blockManager.
            getStoredBlock(new Block(cblock.getBlockId()));
      String reason = findReasonForNotCaching(cblock, blockInfo);
      ReplicationVector neededCachedRV = new ReplicationVector(0);
      if (reason != null) {
        LOG.info("Block {}: can't cache block because it is {}",
            cblock.getBlockId(), reason);
      } else {
        neededCachedRV = cblock.getReplicationVector(); 
      }
      
      // Changed from Elena
      // Call 3 times the computeRV method to take 3 RVs of 
      // cached, pendCached and pendUncached lists
      ReplicationVector cachedRV = computeRV(cached);
      ReplicationVector pendCachedRV = computeRV(pendingCached);
      ReplicationVector pendUncachedRV = computeRV(pendingUncached);
      
      // For loop for all storage types
      for(StorageType st : StorageType.values()){
          int numCached = cachedRV.getReplication(st); 
          int neededCached = neededCachedRV.getReplication(st);
          int numPendingCached = pendCachedRV.getReplication(st);
          int numPendingUncached = pendUncachedRV.getReplication(st);

          if(numCached==0 && neededCached==0 && numPendingCached==0 && numPendingUncached==0){
              continue;
          }
              
      if (numCached >= neededCached) {
        // If we have enough replicas, drop all pending cached.
        for (Iterator<DatanodeStorageInfo> iter = pendingCached.iterator();
            iter.hasNext(); ) {
          DatanodeStorageInfo datanode = iter.next();
          datanode.getPendingCached().remove(cblock);
          iter.remove();
          LOG.trace("Block {}: removing from PENDING_CACHED for node {}"
                  + "because we already have {} cached replicas and we only" +
                  " need {}",
              cblock.getBlockId(), 
              datanode.getDatanodeDescriptor().getDatanodeUuid(), 
              numCached,
              neededCached
          );
        }
      }
      
      if (numCached < neededCached) {
        // If we don't have enough replicas, drop all pending uncached.
        for (Iterator<DatanodeStorageInfo> iter = pendingUncached.iterator();
            iter.hasNext(); ) {
            DatanodeStorageInfo datanode = iter.next();
          datanode.getPendingUncached().remove(cblock);
          iter.remove();
          LOG.trace("Block {}: removing from PENDING_UNCACHED for node {} "
                  + "because we only have {} cached replicas and we need " +
                  "{}", cblock.getBlockId(), datanode.getDatanodeDescriptor().getDatanodeUuid(),
              numCached, neededCached
          );
        }
      }
      
      int neededUncached = numCached - (numPendingUncached + neededCached);
      if (neededUncached > 0) {
        addNewPendingUncached(neededUncached, cblock, cached,
            pendingUncached, st);
      } else {
        int additionalCachedNeeded = neededCached -
            (numCached + numPendingCached);
        if (additionalCachedNeeded > 0) {
          addNewPendingCached(additionalCachedNeeded, cblock, cached,
              pendingCached, st);
        }
      }
      }
          
      if ((neededCachedRV.getTotalReplication() == 0) &&
          pendingUncached.isEmpty() &&
          pendingCached.isEmpty()) {
        // we have nothing more to do with this block.
        LOG.trace("Block {}: removing from cachedBlocks, since neededCached "
                + "== 0, and pendingUncached and pendingCached are empty.",
            cblock.getBlockId()
        );
        cbIter.remove();
      }
    }
  }

  //Added from Elena
  private ReplicationVector computeRV(List<DatanodeStorageInfo> cached) {
      List<StorageType> storages = new ArrayList<StorageType>();

      for (DatanodeStorageInfo dsi : cached) {
          storages.add(dsi.getStorageType());
      }

      return new ReplicationVector(storages);
  }

/**
   * Add new entries to the PendingUncached list.
   *
   * @param neededUncached   The number of replicas that need to be uncached.
   * @param cachedBlock      The block which needs to be uncached.
   * @param cached           A list of DataNodes currently caching the block.
   * @param pendingUncached  A list of DataNodes that will soon uncache the
   *                         block.
   * @param type             Storage type on which to uncache
   */
  private void addNewPendingUncached(int neededUncached,
      CachedBlock cachedBlock, List<DatanodeStorageInfo> cached,
      List<DatanodeStorageInfo> pendingUncached, StorageType type) {
     
    // Figure out which replicas can be uncached.
    ArrayList<DatanodeStorageInfo> possibilities =
        new ArrayList<DatanodeStorageInfo>(cached.size());
    for (DatanodeStorageInfo datanode : cached) {
      if (!pendingUncached.contains(datanode) && 
            datanode.getStorageType().equals(type)) {
        possibilities.add(datanode);
      }
    }
    
    if (neededUncached > possibilities.size()) {
       LOG.warn("Logic error: we're trying to uncache more replicas than " +
           "actually exist for " + cachedBlock + " on " + type.toString());
       return;
    }
    
    // Sort the possibilities on increasing cache remaining size
    Collections.sort(possibilities, new Comparator<DatanodeStorageInfo>() {
       @Override
       public int compare(DatanodeStorageInfo o1, DatanodeStorageInfo o2) {
          long diff = o1.getCacheRemaining() - o2.getCacheRemaining();
          return (diff < 0) ? -1 : ((diff > 0) ? 1 : 0);
       }
    });
    
    for (int i = 0; i < neededUncached; ++i) {
       DatanodeStorageInfo dnStInfo = possibilities.get(i);
       pendingUncached.add(dnStInfo);
       boolean added = dnStInfo.getPendingUncached().add(cachedBlock);
       assert added;
    }
  }
  
  /**
   * Add new entries to the PendingCached list.
   *
   * @param neededCached     The number of replicas that need to be cached.
   * @param cachedBlock      The block which needs to be cached.
   * @param cached           A list of DataNodes currently caching the block.
   * @param pendingCached    A list of DataNodes that will soon cache the
   *                         block.
   * @param type             Storage type on which to cache
   */
  private void addNewPendingCached(final int neededCached,
      CachedBlock cachedBlock, List<DatanodeStorageInfo> cached,
      List<DatanodeStorageInfo> pendingCached, StorageType type) {
    // To figure out which replicas can be cached, we consult the
    // blocksMap.  We don't want to try to cache a corrupt replica, though.
    BlockInfoContiguous blockInfo = blockManager.
          getStoredBlock(new Block(cachedBlock.getBlockId()));
    if (blockInfo == null) {
      LOG.debug("Block {}: can't add new cached replicas," +
          " because there is no record of this block " +
          "on the NameNode.", cachedBlock.getBlockId());
      return;
    }
    if (!blockInfo.isComplete()) {
      LOG.debug("Block {}: can't cache this block, because it is not yet"
          + " complete.", cachedBlock.getBlockId());
      return;
    }
    
    // Determine possible caching locations based the source locations.
    // Group the possibilities on the storage type of the source location.
    EnumMap<StorageType, List<DatanodeStorageInfo>> possibleCacheLocs = 
          new EnumMap<StorageType, List<DatanodeStorageInfo>>(StorageType.class);
    int numReplicas = blockInfo.getCapacity();
    DatanodeDescrTypePairs corrupt = blockManager.getCorruptReplicas(blockInfo);
    Set<DatanodeDescriptor> goodSourceLocs = new HashSet<DatanodeDescriptor>(numReplicas);
    int outOfCapacity = 0;
    
    for (int i = 0; i < numReplicas; i++) {
        DatanodeStorageInfo storage = blockInfo.getStorageInfo(i);
        if (storage == null) {
           continue;
        }
        
        DatanodeDescriptor datanode = storage.getDatanodeDescriptor();
        if (datanode == null) {
           continue;
        }
        
        DatanodeStorageInfo[] storagesToCache = datanode.getStorageInfosType(type);
        StorageType srcType = storage.getStorageType();
        
        if (datanode.isDecommissioned() || datanode.isDecommissionInProgress()) {
            continue;
        }
        if (corrupt != null && corrupt.contains(storage)) {
            continue;
        }
        
        goodSourceLocs.add(datanode);

        if (type.equals(srcType)){
            continue;
        }

        boolean alreadyCached = false;
        for (int k = 0; k < storagesToCache.length; k++) {
            if (pendingCached.contains(storagesToCache[k])
                  || cached.contains(storagesToCache[k])) {
               alreadyCached = true;
               break;
            }
        }
        
        if (alreadyCached) {
           continue;
        }

        for (int j = 0; j < storagesToCache.length; j++) {
             long pendingBytes = 0;
             // Subtract pending cached blocks from effective capacity
             Iterator<CachedBlock> it = storagesToCache[j].getPendingCached().iterator();
             while (it.hasNext()) {
                 CachedBlock cBlock = it.next();
                 BlockInfoContiguous info =
                         blockManager.getStoredBlock(new Block(cBlock.getBlockId()));
                 if (info != null) {
                     pendingBytes -= info.getNumBytes();
                 }
             }
             it = storagesToCache[j].getPendingUncached().iterator();
             // Add pending uncached blocks from effective capacity
             while (it.hasNext()) {
                 CachedBlock cBlock = it.next();
                 BlockInfoContiguous info =
                         blockManager.getStoredBlock(new Block(cBlock.getBlockId()));
                 if (info != null) {
                     pendingBytes += info.getNumBytes();
                 }
             }
             long pendingCapacity = pendingBytes + storagesToCache[j].getCacheRemaining();
             if (pendingCapacity < blockInfo.getNumBytes()) {
                 LOG.trace("Block {}: DataNode storage {} is not a valid possibility " +
                         "because the block has size {}, but the DataNode only has {}" +
                         "bytes of cache remaining ({} pending bytes, {} already cached.",
                         blockInfo.getBlockId(), storagesToCache[j].getStorageID(),
                         blockInfo.getNumBytes(), pendingCapacity, pendingBytes,
                         storagesToCache[j].getCacheRemaining());
                 outOfCapacity++;
                 continue;
             }
             
             if (!possibleCacheLocs.containsKey(srcType))
                possibleCacheLocs.put(srcType, new ArrayList<DatanodeStorageInfo>(1));
             possibleCacheLocs.get(srcType).add(storagesToCache[j]);
        }
    }
    
    // Choose nodes for caching in reverse type order in order to increase
    // the effectiveness of caching
    int remaining = neededCached;
    List<DatanodeStorageInfo> chosen = new ArrayList<DatanodeStorageInfo>(remaining);
    List<DatanodeStorageInfo> chosenPartial;
    StorageType[] allTypes = StorageType.valuesOrdered();
    for (int t = allTypes.length - 1; t >= 0; --t) {
       if (possibleCacheLocs.containsKey(allTypes[t])) {
          chosenPartial = chooseDatanodesForCaching(possibleCacheLocs.get(allTypes[t]),
                remaining, blockManager.getDatanodeManager().getStaleInterval());
          chosen.addAll(chosenPartial);
          remaining -= chosenPartial.size();
          if (remaining <= 0)
             break;
       }
    }
    
    for (DatanodeStorageInfo s : chosen) {
        LOG.trace("Block {}: added to PENDING_CACHED in storage {} on DataNode {}",
                blockInfo.getBlockId(), s.getStorageID(),
                s.getDatanodeDescriptor().getDatanodeUuid());

         pendingCached.add(s);
         boolean added = s.getPendingCached().add(cachedBlock);
         assert added;
    }
    
    // Choose a source datanode. Give preferences to datanodes that will cache
    // the replica. Start iterating from the faster source storage types.
    DatanodeDescriptor srcDN = null;
    if (!goodSourceLocs.isEmpty() && !chosen.isEmpty()) {
       for (int i = chosen.size() - 1; i >= 0; --i) {
          if (goodSourceLocs.contains(chosen.get(i).getDatanodeDescriptor())) {
             srcDN = chosen.get(i).getDatanodeDescriptor();
             break;
          }
       }
       
       if (srcDN == null && !goodSourceLocs.isEmpty())
          srcDN = goodSourceLocs.iterator().next();
       
       if (srcDN != null)
          srcDN.addBlockToCache(new Block(blockInfo),
                chosen.toArray(new DatanodeStorageInfo[chosen.size()]));
    }    
    
    // We were unable to satisfy the requested replication factor
    if (neededCached > chosen.size()) { 
      LOG.debug("Block {}: we only have {} of {} cached replicas."
              + " {} DataNodes have insufficient cache capacity.",
          blockInfo.getBlockId(),
          (cachedBlock.getReplication(type) - neededCached + chosen.size()),
          cachedBlock.getReplication(type), outOfCapacity
      );
    }
  }

  /**
   * Chooses datanode locations for caching from a list of valid possibilities.
   * Non-stale nodes are chosen before stale nodes.
   * 
   * @param possibilities List of candidate datanodes
   * @param neededCached Number of replicas needed
   * @param staleInterval Age of a stale datanode
   * @return A list of chosen datanodes
   */
  private static List<DatanodeStorageInfo> chooseDatanodesForCaching(
      final List<DatanodeStorageInfo> possibilities, final int neededCached,
      final long staleInterval) {
    // Make a copy that we can modify
    List<DatanodeStorageInfo> targets =
        new ArrayList<DatanodeStorageInfo>(possibilities);
    // Selected targets
    List<DatanodeStorageInfo> chosen = new LinkedList<DatanodeStorageInfo>();

    // Filter out stale datanodes
    List<DatanodeStorageInfo> stale = new LinkedList<DatanodeStorageInfo>();
    Iterator<DatanodeStorageInfo> it = targets.iterator();
    while (it.hasNext()) {
    	DatanodeStorageInfo d = it.next();
      if (d.getDatanodeDescriptor().isStale(staleInterval)) {
        it.remove();
        stale.add(d);
      }
    }
    // Select targets
    while (chosen.size() < neededCached) {
      // Try to use stale nodes if we're out of non-stale nodes, else we're done
      if (targets.isEmpty()) {
        if (!stale.isEmpty()) {
          targets = stale;
        } else {
          break;
        }
      }
      // Select a random target
      DatanodeStorageInfo target =
          chooseRandomDatanodeByRemainingCapacity(targets);
      chosen.add(target);
      targets.remove(target);
    }
    return chosen;
  }

  /**
   * Choose a single datanode from the provided list of possible
   * targets, weighted by the percentage of free space remaining on the node.
   * 
   * @return The chosen datanode
   */
  private static DatanodeStorageInfo chooseRandomDatanodeByRemainingCapacity(
      final List<DatanodeStorageInfo> targets) {
    // Use a weighted probability to choose the target datanode
    float total = 0;
    for (DatanodeStorageInfo d : targets) {
      total += d.getCacheRemainingPercent();
    }
    // Give each datanode a portion of keyspace equal to its relative weight
    // [0, w1) selects d1, [w1, w2) selects d2, etc.
    TreeMap<Integer, DatanodeStorageInfo> lottery =
        new TreeMap<Integer, DatanodeStorageInfo>();
    int offset = 0;
    for (DatanodeStorageInfo d : targets) {
      // Since we're using floats, be paranoid about negative values
      int weight =
          Math.max(1, (int)((d.getCacheRemainingPercent() / total) * 1000000));
      offset += weight;
      lottery.put(offset, d);
    }
    // Choose a number from [0, offset), which is the total amount of weight,
    // to select the winner
    DatanodeStorageInfo winner =
        lottery.higherEntry(random.nextInt(offset)).getValue();
    return winner;
  }
}
