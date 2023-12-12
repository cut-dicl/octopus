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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.EnumMap;
import java.util.Map.Entry;

import javax.annotation.Nonnull;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.CacheDirective;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolStats;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.IntrusiveCollection;

import com.google.common.base.Preconditions;

/**
 * A CachePool describes a set of cache resources being managed by the NameNode.
 * User caching requests are billed to the cache pool specified in the request.
 *
 * This is an internal class, only used on the NameNode.  For identifying or
 * describing a cache pool to clients, please use CachePoolInfo.
 * 
 * CachePools must be accessed under the FSNamesystem lock.
 */
@InterfaceAudience.Private
public final class CachePool {
  @Nonnull
  private final String poolName;

  @Nonnull
  private String ownerName;

  @Nonnull
  private String groupName;
  
  /**
   * Cache pool permissions.
   * 
   * READ permission means that you can list the cache directives in this pool.
   * WRITE permission means that you can add, remove, or modify cache directives
   *       in this pool.
   * EXECUTE permission is unused.
   */
  @Nonnull
  private FsPermission mode;

  /**
   * Maximum number of bytes that can be cached in this pool per storage type.
   */
  private EnumMap<StorageType, Long> limitPerType;

  /**
   * Maximum duration that a CacheDirective in this pool remains valid,
   * in milliseconds.
   */
  private long maxRelativeExpiryMs;

  private static class CachePoolStatsInternal{
	  long bytesNeeded = 0L;
	  long bytesCached = 0L;
	  long filesNeeded = 0L;
	  long filesCached = 0L;
	  
	  @Override
	  public String toString() {
	    StringBuilder builder = new StringBuilder();
	    builder.append("{");
	    builder.append("bytesNeeded: ").append(bytesNeeded);
	    builder.append(", ").append("bytesCached: ").append(bytesCached);
	    builder.append(", ").append("filesNeeded: ").append(filesNeeded);
	    builder.append(", ").append("filesCached: ").append(filesCached);
	    builder.append("}");
	    return builder.toString();
	  }
  }

  private EnumMap<StorageType, CachePoolStatsInternal> statsPerType;

  public final static class DirectiveList
      extends IntrusiveCollection<CacheDirective> {
    private final CachePool cachePool;

    private DirectiveList(CachePool cachePool) {
      this.cachePool = cachePool;
    }

    public CachePool getCachePool() {
      return cachePool;
    }
  }

  @Nonnull
  private final DirectiveList directiveList = new DirectiveList(this);

  /**
   * Create a new cache pool based on a CachePoolInfo object and the defaults.
   * We will fill in information that was not supplied according to the
   * defaults.
   */
  static CachePool createFromInfoAndDefaults(CachePoolInfo info)
      throws IOException {
    UserGroupInformation ugi = null;
    String ownerName = info.getOwnerName();
    if (ownerName == null) {
      ugi = NameNode.getRemoteUser();
      ownerName = ugi.getShortUserName();
    }
    String groupName = info.getGroupName();
    if (groupName == null) {
      if (ugi == null) {
        ugi = NameNode.getRemoteUser();
      }
      groupName = ugi.getPrimaryGroupName();
    }
    FsPermission mode = (info.getMode() == null) ? 
        FsPermission.getCachePoolDefault() : info.getMode();
        
    EnumMap<StorageType, Long> limitPerType = info.getLimits();
    if (limitPerType.isEmpty()) {
        // Default: unlimited memory cache
        limitPerType.put(StorageType.MEMORY, CachePoolInfo.DEFAULT_LIMIT);
    }

    long maxRelativeExpiry = info.getMaxRelativeExpiryMs() == null ?
        CachePoolInfo.DEFAULT_MAX_RELATIVE_EXPIRY :
        info.getMaxRelativeExpiryMs();
        
    return new CachePool(info.getPoolName(),
        ownerName, groupName, mode, limitPerType, maxRelativeExpiry);
  }

  /**
   * Create a new cache pool based on a CachePoolInfo object.
   * No fields in the CachePoolInfo can be blank.
   */
  static CachePool createFromInfo(CachePoolInfo info) {
    return new CachePool(info.getPoolName(),
        info.getOwnerName(), info.getGroupName(),
        info.getMode(), info.getLimits(),info.getMaxRelativeExpiryMs());
  }

  CachePool(String poolName, String ownerName, String groupName,
      FsPermission mode, EnumMap<StorageType, Long> limitPerType, long maxRelativeExpiry) {
    Preconditions.checkNotNull(poolName);
    Preconditions.checkNotNull(ownerName);
    Preconditions.checkNotNull(groupName);
    Preconditions.checkNotNull(mode);
    this.poolName = poolName;
    this.ownerName = ownerName;
    this.groupName = groupName;
    this.mode = new FsPermission(mode);
    this.limitPerType = limitPerType;
    this.maxRelativeExpiryMs = maxRelativeExpiry;
    this.statsPerType = new EnumMap<>(StorageType.class);
  }

  public String getPoolName() {
    return poolName;
  }

  public String getOwnerName() {
    return ownerName;
  }

  public CachePool setOwnerName(String ownerName) {
    this.ownerName = ownerName;
    return this;
  }

  public String getGroupName() {
    return groupName;
  }

  public CachePool setGroupName(String groupName) {
    this.groupName = groupName;
    return this;
  }

  public FsPermission getMode() {
    return mode;
  }

  public CachePool setMode(FsPermission mode) {
    this.mode = new FsPermission(mode);
    return this;
  }

  public Long getLimit(StorageType storage) {
      if (limitPerType.containsKey(storage)) {
          return limitPerType.get(storage);
      }
      return null; 
  }

  public CachePool setLimit(long bytes, StorageType storage) {
    this.limitPerType.put(storage, bytes);
    return this;
  }

  public EnumMap<StorageType, Long> getLimits(){
	  return this.limitPerType; 
  }
  
  public CachePool setLimits(EnumMap<StorageType, Long> limitPerType){
	  this.limitPerType = limitPerType;
	  return this;
  }
  
  public long getMaxRelativeExpiryMs() {
    return maxRelativeExpiryMs;
  }

  public CachePool setMaxRelativeExpiryMs(long expiry) {
    this.maxRelativeExpiryMs = expiry;
    return this;
  }

  /**
   * Get either full or partial information about this CachePool.
   *
   * @param fullInfo
   *          If true, only the name will be returned (i.e., what you 
   *          would get if you didn't have read permission for this pool.)
   * @return
   *          Cache pool information.
   */
  CachePoolInfo getInfo(boolean fullInfo) {
    CachePoolInfo info = new CachePoolInfo(poolName);
    if (!fullInfo) {
      return info;
    }
    return info.setOwnerName(ownerName).
        setGroupName(groupName).
        setMode(new FsPermission(mode)).
        setLimits(new EnumMap<StorageType, Long>(limitPerType)).
        setMaxRelativeExpiryMs(maxRelativeExpiryMs);
  }

  /**
   * Resets statistics related to this CachePool
   */
  public void resetStatistics() {
      for(StorageType type : statsPerType.keySet()){
          statsPerType.get(type).bytesNeeded = 0;
          statsPerType.get(type).bytesCached = 0;
          statsPerType.get(type).filesNeeded = 0;
          statsPerType.get(type).filesCached = 0;
      }
  }

  public void addBytesNeeded(long bytes, StorageType type) {
      if (!statsPerType.containsKey(type))
          statsPerType.put(type, new CachePoolStatsInternal());
      
	  statsPerType.get(type).bytesNeeded += bytes;
  }

  public void addBytesCached(long bytes, StorageType type) {
      if (!statsPerType.containsKey(type))
          statsPerType.put(type, new CachePoolStatsInternal());
      
	  statsPerType.get(type).bytesCached += bytes;
  }

  public void addFilesNeeded(long files, StorageType type) {
      if (!statsPerType.containsKey(type))
          statsPerType.put(type, new CachePoolStatsInternal());
      
	  statsPerType.get(type).filesNeeded += files;
  }

  public void addFilesCached(long files, StorageType type) {
      if (!statsPerType.containsKey(type))
          statsPerType.put(type, new CachePoolStatsInternal());
      
	  statsPerType.get(type).filesCached += files;
  }

  public long getBytesNeeded(StorageType type) {
      if (statsPerType.containsKey(type))
             return statsPerType.get(type).bytesNeeded;
      else
          return 0;
  }

  public long getBytesCached(StorageType type) {
      if (statsPerType.containsKey(type))
          return statsPerType.get(type).bytesCached;
      else
          return 0;
  }

  public long getBytesOverlimit(StorageType type) {
      if (statsPerType.containsKey(type)) {
         long limit = 0;
         if (limitPerType.containsKey(type))
            limit = limitPerType.get(type);
         return Math.max(statsPerType.get(type).bytesNeeded - limit, 0);
      }
      else
         return 0;
  }

  public long getFilesNeeded(StorageType type) {
      if (statsPerType.containsKey(type))
          return statsPerType.get(type).filesNeeded;
      else
          return 0;
  }

  public long getFilesCached(StorageType type) {
      if (statsPerType.containsKey(type))
          return statsPerType.get(type).filesCached;
      else
          return 0;
  }

  /**
   * Get statistics about this CachePool.
   *
   * @return   Cache pool statistics.
   */
   private EnumMap<StorageType, CachePoolStats> getStats() {
	  EnumMap<StorageType, CachePoolStats> stats = 
	        new EnumMap<StorageType, CachePoolStats>(StorageType.class);

	  for(Entry<StorageType, CachePoolStatsInternal> s : statsPerType.entrySet()){
		  stats.put(s.getKey(), new CachePoolStats.Builder().
				  setBytesNeeded(s.getValue().bytesNeeded).
				  setBytesCached(s.getValue().bytesCached).
				  setBytesOverlimit(getBytesOverlimit(s.getKey())).
				  setFilesNeeded(s.getValue().filesNeeded).
				  setFilesCached(s.getValue().filesCached).
				  setStorageType(s.getKey()).
				  build());
	  }

	  return stats;
  }

  /**
   * Returns a CachePoolInfo describing this CachePool based on the permissions
   * of the calling user. Unprivileged users will see only minimal descriptive
   * information about the pool.
   * 
   * @param pc Permission checker to be used to validate the user's permissions,
   *          or null
   * @return CachePoolEntry describing this CachePool
   */
  public CachePoolEntry getEntry(FSPermissionChecker pc) {
    boolean hasPermission = true;
    if (pc != null) {
      try {
        pc.checkPermission(this, FsAction.READ);
      } catch (AccessControlException e) {
        hasPermission = false;
      }
    }
    return new CachePoolEntry(getInfo(hasPermission), 
        hasPermission ? getStats() : new EnumMap<StorageType, CachePoolStats>(StorageType.class)); 
  }

  public String toString() {
    return new StringBuilder().
        append("{ ").append("poolName:").append(poolName).
        append(", ownerName:").append(ownerName).
        append(", groupName:").append(groupName).
        append(", mode:").append(mode).
        append(", limit:").append(limitPerType).
        append(", maxRelativeExpiryMs:").append(maxRelativeExpiryMs).
        append(" }").toString();
  }

  public DirectiveList getDirectiveList() {
    return directiveList;
  }
}
