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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.fs.ReplicationVector;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfoWithStorage;
import org.apache.hadoop.hdfs.server.namenode.CachedBlock;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage.State;
import org.apache.hadoop.util.IntrusiveCollection;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;

/**
 * A Datanode has one or more storages. A storage in the Datanode is represented
 * by this class.
 */
public class DatanodeStorageInfo {
  public static final DatanodeStorageInfo[] EMPTY_ARRAY = {};

  public static DatanodeInfo[] toDatanodeInfos(DatanodeStorageInfo[] storages) {
    return toDatanodeInfos(Arrays.asList(storages));
  }
  static DatanodeInfo[] toDatanodeInfos(List<DatanodeStorageInfo> storages) {
    final DatanodeInfo[] datanodes = new DatanodeInfo[storages.size()];
    for(int i = 0; i < storages.size(); i++) {
      datanodes[i] = storages.get(i).getDatanodeDescriptor();
    }
    return datanodes;
  }

  static DatanodeDescriptor[] toDatanodeDescriptors(
      DatanodeStorageInfo[] storages) {
    DatanodeDescriptor[] datanodes = new DatanodeDescriptor[storages.length];
    for (int i = 0; i < storages.length; ++i) {
      datanodes[i] = storages[i].getDatanodeDescriptor();
    }
    return datanodes;
  }

  //Added from Elena
  public static DatanodeInfoWithStorage[] toDatanodeInfoWithStorage(List<DatanodeStorageInfo> storages){
     if (storages == null || storages.size() == 0)
        return null;
     
     DatanodeInfoWithStorage[] arrStorages = new DatanodeInfoWithStorage[storages.size()];
     DatanodeStorageInfo storage;
     
     for (int i = 0; i <arrStorages.length; ++i) {
        storage = storages.get(i);
        arrStorages[i] = new DatanodeInfoWithStorage(storage.getDatanodeDescriptor(), 
              storage.getStorageID(), storage.getStorageType());
     }
     
     return arrStorages;
  }
  
  
  public static String[] toStorageIDs(DatanodeStorageInfo[] storages) {
    String[] storageIDs = new String[storages.length];
    for(int i = 0; i < storageIDs.length; i++) {
      storageIDs[i] = storages[i].getStorageID();
    }
    return storageIDs;
  }

  public static StorageType[] toStorageTypes(DatanodeStorageInfo[] storages) {
    StorageType[] storageTypes = new StorageType[storages.length];
    for(int i = 0; i < storageTypes.length; i++) {
      storageTypes[i] = storages[i].getStorageType();
    }
    return storageTypes;
  }

  public void updateFromStorage(DatanodeStorage storage) {
    state = storage.getState();
    storageType = storage.getStorageType();
  }

  /**
   * Iterates over the list of blocks belonging to the data-node.
   */
  class BlockIterator implements Iterator<BlockInfoContiguous> {
    private BlockInfoContiguous current;

    BlockIterator(BlockInfoContiguous head) {
      this.current = head;
    }

    public boolean hasNext() {
      return current != null;
    }

    public BlockInfoContiguous next() {
      BlockInfoContiguous res = current;
      current = current.getNext(current.findStorageInfo(DatanodeStorageInfo.this));
      return res;
    }

    public void remove() {
      throw new UnsupportedOperationException("Sorry. can't remove.");
    }
  }

  private final DatanodeDescriptor dn;
  private final String storageID;
  private StorageType storageType;
  private State state;

  private long capacity;
  private long dfsUsed;
  private volatile long remaining;
  private long blockPoolUsed;
  
  //Added from Elena
  private long cacheCapacity;
  private long cacheUsed;
  private long cacheRemaining;
  private long cacheBpUsed;
  
  private long writeThroughput;
  private long readThroughput;
  private int readerCount; // as reported by DN
  private int writerCount; // as reported by DN
  
  private AtomicInteger readsScheduled;
  
  private volatile BlockInfoContiguous blockList = null;
  private int numBlocks = 0;

  // The ID of the last full block report which updated this storage.
  private long lastBlockReportId = 0;

  /** The number of block reports received */
  private int blockReportCount = 0;

  /**
   * Set to false on any NN failover, and reset to true
   * whenever a block report is received.
   */
  private boolean heartbeatedSinceFailover = false;

  /**
   * At startup or at failover, the storages in the cluster may have pending
   * block deletions from a previous incarnation of the NameNode. The block
   * contents are considered as stale until a block report is received. When a
   * storage is considered as stale, the replicas on it are also considered as
   * stale. If any block has at least one stale replica, then no invalidations
   * will be processed for this block. See HDFS-1972.
   */
  private boolean blockContentsStale = true;

  DatanodeStorageInfo(DatanodeDescriptor dn, DatanodeStorage s) {
    this.dn = dn;
    this.storageID = s.getStorageID();
    this.storageType = s.getStorageType();
    this.state = s.getState();
    this.readsScheduled = new AtomicInteger(0);
  }

  int getBlockReportCount() {
    return blockReportCount;
  }

  void setBlockReportCount(int blockReportCount) {
    this.blockReportCount = blockReportCount;
  }

  boolean areBlockContentsStale() {
    return blockContentsStale;
  }

  void markStaleAfterFailover() {
    heartbeatedSinceFailover = false;
    blockContentsStale = true;
  }

  void receivedHeartbeat(StorageReport report) {
    updateState(report);
    heartbeatedSinceFailover = true;
  }

  void receivedBlockReport() {
    if (heartbeatedSinceFailover) {
      blockContentsStale = false;
    }
    blockReportCount++;
  }

  @VisibleForTesting
  public void setUtilizationForTesting(long capacity, long dfsUsed,
                      long remaining, long blockPoolUsed) {
    this.capacity = capacity;
    this.dfsUsed = dfsUsed;
    this.remaining = remaining;
    this.blockPoolUsed = blockPoolUsed;
  }

  @VisibleForTesting
  public void setReaderCountForTesting(int readerCount) {
    this.readerCount = readerCount;
    int oldReaderCount = readsScheduled.getAndSet(readerCount);
    dn.updateApproxReadsScheduled(readerCount - oldReaderCount);
  }

  long getLastBlockReportId() {
    return lastBlockReportId;
  }

  void setLastBlockReportId(long lastBlockReportId) {
    this.lastBlockReportId = lastBlockReportId;
  }

  State getState() {
    return this.state;
  }

  void setState(State state) {
    this.state = state;
  }

  boolean areBlocksOnFailedStorage() {
    return getState() == State.FAILED && numBlocks != 0;
  }

  String getStorageID() {
    return storageID;
  }

  public StorageType getStorageType() {
    return storageType;
  }

  long getCapacity() {
    return capacity;
  }

  long getDfsUsed() {
    return dfsUsed;
  }

  long getRemaining() {
    return remaining;
  }

  long getBlockPoolUsed() {
    return blockPoolUsed;
  }

  //Added from elena
  long getWriteThroughput() {
	  return writeThroughput;
  }
  long getReadThroughput() {
	  return readThroughput;
  }
 
  //Added from Elena
  /**
   * @return Amount of cache capacity in bytes
   */
  public long getCacheCapacity() {
    return cacheCapacity;
  }

  /**
   * @return Amount of cache used in bytes
   */
  public long getCacheUsed() {
    return cacheUsed;
  }

  /**
   * @return Cache used as a percentage of the datanode's total cache capacity
   */
  public float getCacheUsedPercent() {
    return DFSUtil.getPercentUsed(cacheUsed, cacheCapacity);
  }

  /**
   * @return Amount of cache remaining in bytes
   */
  public long getCacheRemaining() {
    return cacheRemaining;
  }

  public long getCacheBpUsed() {
      return cacheBpUsed;
  }

  public int getXceiverCount() {
     return readerCount + writerCount;
  }
  
  public int getReaderCount() {
     return readerCount;
  }
  
  public int getWriterCount() {
     return writerCount;
  }

  public void incrReadsScheduled() {
     readsScheduled.incrementAndGet();
     dn.updateApproxReadsScheduled(1);
  }
  
  public int getApproXceiverCount() {
     return readsScheduled.get()
            + dn.getBlocksScheduledPerStorage(storageType);
  }

  /**
   * @return Cache remaining as a percentage of the datanode's total cache
   * capacity
   */
  public float getCacheRemainingPercent() {
    return DFSUtil.getPercentRemaining(getCacheRemaining(), cacheCapacity);
  }
  
  public AddBlockResult addBlock(BlockInfoContiguous b) {
    // First check whether the block belongs to a different storage
    // on the same DN.
    AddBlockResult result = AddBlockResult.ADDED;
    DatanodeStorageInfo otherStorage =
        b.findStorageInfo(getDatanodeDescriptor(), storageType);

    if (otherStorage != null) {
      if (otherStorage != this) {
        // The block belongs to a different storage. Remove it first.
        otherStorage.removeBlock(b);
        result = AddBlockResult.REPLACED;
      } else {
        // The block is already associated with this storage.
        return AddBlockResult.ALREADY_EXIST;
      }
    }

    // add to the head of the data-node list
    b.addStorage(this);
    blockList = b.listInsert(blockList, this);
    numBlocks++;
    return result;
  }

  public boolean removeBlock(BlockInfoContiguous b) {
    blockList = b.listRemove(blockList, this);
    if (b.removeStorage(this)) {
      numBlocks--;
      return true;
    } else {
      return false;
    }
  }

  int numBlocks() {
    return numBlocks;
  }
  
  Iterator<BlockInfoContiguous> getBlockIterator() {
    return new BlockIterator(blockList);

  }

  /**
   * Move block to the head of the list of blocks belonging to the data-node.
   * @return the index of the head of the blockList
   */
  int moveBlockToHead(BlockInfoContiguous b, int curIndex, int headIndex) {
    blockList = b.moveBlockToHead(blockList, this, curIndex, headIndex);
    return curIndex;
  }

  /**
   * Used for testing only
   * @return the head of the blockList
   */
  @VisibleForTesting
  BlockInfoContiguous getBlockListHeadForTesting(){
    return blockList;
  }

  void updateState(StorageReport r) {
    capacity = r.getCapacity();
    dfsUsed = r.getDfsUsed();
    remaining = r.getRemaining();
    blockPoolUsed = r.getBlockPoolUsed();

    cacheCapacity = r.getCacheCapacity();
    cacheUsed = r.getCacheUsed();
    cacheRemaining = r.getCacheRemaining();
    cacheBpUsed = r.getCacheBpUsed();

    writeThroughput = r.getWriteThroughput();
    readThroughput = r.getReadThroughput();

    readerCount = r.getReaderCount();
    writerCount = r.getWriterCount();
    
    int oldReaderCount = readsScheduled.getAndSet(readerCount);
    dn.updateApproxReadsScheduled(readerCount - oldReaderCount);
  }

  public DatanodeDescriptor getDatanodeDescriptor() {
    return dn;
  }

  /** Increment the number of blocks scheduled for each given storage */ 
  public static void incrementBlocksScheduled(DatanodeStorageInfo... storages) {
    for (DatanodeStorageInfo s : storages) {
      s.getDatanodeDescriptor().incrementBlocksScheduled(s.getStorageType());
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj == null || !(obj instanceof DatanodeStorageInfo)) {
      return false;
    }
    final DatanodeStorageInfo that = (DatanodeStorageInfo)obj;
    return this.storageID.equals(that.storageID);
  }

  @Override
  public int hashCode() {
    return storageID.hashCode();
  }

  @Override
  public String toString() {
    return "[" + storageType + "]" + storageID + ":" + state + ":" + dn;
  }
  
  StorageReport toStorageReport() {
    return new StorageReport(
        new DatanodeStorage(storageID, state, storageType),
        false, capacity, dfsUsed, remaining, blockPoolUsed, 
        writeThroughput, readThroughput,
        cacheCapacity, cacheUsed, cacheRemaining, cacheBpUsed, 
        readerCount, writerCount);
  }

  static Iterable<StorageType> toStorageTypes(
      final Iterable<DatanodeStorageInfo> infos) {
    return new Iterable<StorageType>() {
        @Override
        public Iterator<StorageType> iterator() {
          return new Iterator<StorageType>() {
            final Iterator<DatanodeStorageInfo> i = infos.iterator();
            @Override
            public boolean hasNext() {return i.hasNext();}
            @Override
            public StorageType next() {return i.next().getStorageType();}
            @Override
            public void remove() {
              throw new UnsupportedOperationException();
            }
          };
        }
      };
  }

  /** @return the first {@link DatanodeStorageInfo} corresponding to
   *          the given datanode
   */
  static DatanodeStorageInfo getDatanodeStorageInfo(
      final Iterable<DatanodeStorageInfo> infos,
      final DatanodeDescriptor datanode) {
    if (datanode == null) {
      return null;
    }
    for(DatanodeStorageInfo storage : infos) {
      if (storage.getDatanodeDescriptor() == datanode) {
        return storage;
      }
    }
    return null;
  }

  static enum AddBlockResult {
    ADDED, REPLACED, ALREADY_EXIST;
  }
  
  /**
   * Create a replication vector the corresponds to the provided list of storage
   * infos
   * 
   * @param infos
   * @return
   */
  public static ReplicationVector createReplicationVector(
      Iterable<DatanodeStorageInfo> infos) {

    ReplicationVector rv = new ReplicationVector(0l);
    for (DatanodeStorageInfo info : infos) {
      rv.incrReplication(info.storageType, (short) 1);
    }

    return rv;
  }

  /**
   * Create a replication vector the corresponds to the provided list of storage
   * infos
   * 
   * @param infos
   * @return
   */
  public static ReplicationVector createReplicationVector(
      DatanodeStorageInfo[] infos) {

    ReplicationVector rv = new ReplicationVector(0l);
    for (DatanodeStorageInfo info : infos) {
      rv.incrReplication(info.storageType, (short) 1);
    }

    return rv;
  }
  
  //Moved and changed from Elena 
  /**
   * A list of CachedBlock objects on this datanode.
   */
  public static class CachedBlocksList extends IntrusiveCollection<CachedBlock> {
    public enum Type {
      PENDING_CACHED,
      CACHED,
      PENDING_UNCACHED
    }

    private final DatanodeStorageInfo datanodeStorageInfo;

    private final Type type;

    CachedBlocksList(DatanodeStorageInfo datanodeStorageInfo, Type type) {
      this.datanodeStorageInfo = datanodeStorageInfo;
      this.type = type;
    }

    public DatanodeStorageInfo getDatanodeStorageInfo() {
      return datanodeStorageInfo;
    }

    public Type getType() {
      return type;
    }
  }

  /**
   * The blocks which we want to cache on this DataNode.
   */
  private final CachedBlocksList pendingCached = 
      new CachedBlocksList(this, CachedBlocksList.Type.PENDING_CACHED);

  /**
   * The blocks which we know are cached on this datanode.
   * This list is updated by periodic cache reports.
   */
  private final CachedBlocksList cached = 
      new CachedBlocksList(this, CachedBlocksList.Type.CACHED);

  /**
   * The blocks which we want to uncache on this DataNode.
   */
  private final CachedBlocksList pendingUncached = 
      new CachedBlocksList(this, CachedBlocksList.Type.PENDING_UNCACHED);

  public CachedBlocksList getPendingCached() {
    return pendingCached;
  }

  public CachedBlocksList getCached() {
    return cached;
  }

  public CachedBlocksList getPendingUncached() {
    return pendingUncached;
  }

  //Added from Elena
  public void clearCachedBlocksLists(){
      this.pendingCached.clear();
      this.cached.clear();
      this.pendingUncached.clear();
  }
}
