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
package org.apache.hadoop.hdfs.protocol;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Date;
import java.util.EnumMap;
import java.util.Map.Entry;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ReplicationVector;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.namenode.CachePool;
import org.apache.hadoop.util.IntrusiveCollection;
import org.apache.hadoop.util.IntrusiveCollection.Element;

import com.google.common.base.Preconditions;

/**
 * Namenode class that tracks state related to a cached path.
 *
 * This is an implementation class, not part of the public API.
 */
@InterfaceAudience.Private
public final class CacheDirective implements IntrusiveCollection.Element {
  private final long id;
  private final String path;
  private final long replVector;
  private CachePool pool;
  private final long expiryTime;

  private static class CacheDirectiveStatsInternal{
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

  private EnumMap<StorageType, CacheDirectiveStatsInternal> statsPerType;

  private Element prev;
  private Element next;

  public CacheDirective(CacheDirectiveInfo info) {
    this(
        info.getId(),
        info.getPath().toUri().getPath(),
        info.getReplVector(),
        info.getExpiration().getAbsoluteMillis());
  }

  public CacheDirective(long id, String path,
		  long replVector, long expiryTime) {
    Preconditions.checkArgument(id > 0);
    this.id = id;
    this.path = checkNotNull(path);
    Preconditions.checkArgument(replVector > 0);
    this.replVector = replVector;
    this.expiryTime = expiryTime;
    this.statsPerType = new EnumMap<StorageType, CacheDirectiveStatsInternal>(StorageType.class);
    
    ReplicationVector rv = new ReplicationVector(replVector);
    for (StorageType t : StorageType.values()) {
        if (rv.getReplication(t) > 0)
            this.statsPerType.put(t, new CacheDirectiveStatsInternal());
    }
  }

  public long getId() {
    return id;
  }

  public String getPath() {
    return path;
  }

  public long getReplVector() {
    return replVector;
  }

  public CachePool getPool() {
    return pool;
  }

  /**
   * @return When this directive expires, in milliseconds since Unix epoch
   */
  public long getExpiryTime() {
    return expiryTime;
  }

  /**
   * @return When this directive expires, as an ISO-8601 formatted string.
   */
  public String getExpiryTimeString() {
    return DFSUtil.dateToIso8601String(new Date(expiryTime));
  }

  /**
   * Returns a {@link CacheDirectiveInfo} based on this CacheDirective.
   * <p>
   * This always sets an absolute expiry time, never a relative TTL.
   */
  public CacheDirectiveInfo toInfo() {
    return new CacheDirectiveInfo.Builder().
        setId(id).
        setPath(new Path(path)).
        setReplVector(replVector).
        setPool(pool.getPoolName()).
        setExpiration(CacheDirectiveInfo.Expiration.newAbsolute(expiryTime)).
        build();
  }

  public EnumMap<StorageType, CacheDirectiveStats> toStats() {
	  EnumMap<StorageType, CacheDirectiveStats> stats = 
	        new EnumMap<StorageType, CacheDirectiveStats>(StorageType.class);

	  for(Entry<StorageType, CacheDirectiveStatsInternal> s : statsPerType.entrySet()){
		  stats.put(s.getKey(), new CacheDirectiveStats.Builder().
		        setStorageType(s.getKey()).
				  setBytesNeeded(s.getValue().bytesNeeded).
				  setBytesCached(s.getValue().bytesCached).
				  setFilesNeeded(s.getValue().filesNeeded).
				  setFilesCached(s.getValue().filesCached).
				  setHasExpired(new Date().getTime() > expiryTime).
				  build());
	  }
	  return stats;
  }

  public CacheDirectiveEntry toEntry() {
    return new CacheDirectiveEntry(toInfo(), toStats());
  }
  
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("{ id:").append(id).
      append(", path:").append(path).
      append(", replVector:").append(ReplicationVector.StringifyReplVector(replVector)).
      append(", pool:").append(pool).
      append(", expiryTime: ").append(getExpiryTimeString()).
      append(", statsPerType: ").append(statsPerType).
      append(" }");
    return builder.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) { return false; }
    if (o == this) { return true; }
    if (o.getClass() != this.getClass()) {
      return false;
    }
    CacheDirective other = (CacheDirective)o;
    return id == other.id;
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(id).toHashCode();
  }

  //
  // Stats related getters and setters
  //

  /**
   * Resets the byte and file statistics being tracked by this CacheDirective.
   */
  public void resetStatistics() {
      for(StorageType type : statsPerType.keySet()){
          statsPerType.get(type).bytesNeeded = 0;
          statsPerType.get(type).bytesCached = 0;
          statsPerType.get(type).filesNeeded = 0;
          statsPerType.get(type).filesCached = 0;
      }
  }

  public long getBytesNeeded(StorageType type) {
      if (statsPerType.containsKey(type))          
          return statsPerType.get(type).bytesNeeded;
      else
          return 0;
  }

  public void addBytesNeeded(long bytes, StorageType type) {
      if (!statsPerType.containsKey(type))
          statsPerType.put(type, new CacheDirectiveStatsInternal());

      this.statsPerType.get(type).bytesNeeded += bytes;
      pool.addBytesNeeded(bytes, type);
  }

  public long getBytesCached(StorageType type) {
      if (statsPerType.containsKey(type))     
          return statsPerType.get(type).bytesCached;
      else
          return 0;
  }

  public void addBytesCached(long bytes, StorageType type) {
      if (!statsPerType.containsKey(type))
          statsPerType.put(type, new CacheDirectiveStatsInternal());

      this.statsPerType.get(type).bytesCached += bytes;
      pool.addBytesCached(bytes, type);
  }

  public long getFilesNeeded(StorageType type) {
      if (statsPerType.containsKey(type))    
          return statsPerType.get(type).filesNeeded;
      else
          return 0; 
  }

  public void addFilesNeeded(long files, StorageType type) {
      if (!statsPerType.containsKey(type))
          statsPerType.put(type, new CacheDirectiveStatsInternal());

      this.statsPerType.get(type).filesNeeded += files;
      pool.addFilesNeeded(files, type);
  }

  public long getFilesCached(StorageType type) {
      if (statsPerType.containsKey(type))    
          return statsPerType.get(type).filesCached;
      else
          return 0; 
  }

  public void addFilesCached(long files, StorageType type) {
      if (!statsPerType.containsKey(type))
          statsPerType.put(type, new CacheDirectiveStatsInternal());

      this.statsPerType.get(type).filesCached += files;
      pool.addFilesCached(files, type);
  }

  //
  // IntrusiveCollection.Element implementation
  //

  @Override // IntrusiveCollection.Element
  public void insertInternal(IntrusiveCollection<? extends Element> list,
      Element prev, Element next) {
    assert this.pool == null;
    this.pool = ((CachePool.DirectiveList)list).getCachePool();
    this.prev = prev;
    this.next = next;
  }

  @Override // IntrusiveCollection.Element
  public void setPrev(IntrusiveCollection<? extends Element> list, Element prev) {
    assert list == pool.getDirectiveList();
    this.prev = prev;
  }

  @Override // IntrusiveCollection.Element
  public void setNext(IntrusiveCollection<? extends Element> list, Element next) {
    assert list == pool.getDirectiveList();
    this.next = next;
  }

  @Override // IntrusiveCollection.Element
  public void removeInternal(IntrusiveCollection<? extends Element> list) {
    assert list == pool.getDirectiveList();
    this.pool = null;
    this.prev = null;
    this.next = null;
  }

  @Override // IntrusiveCollection.Element
  public Element getPrev(IntrusiveCollection<? extends Element> list) {
    if (list != pool.getDirectiveList()) {
      return null;
    }
    return this.prev;
  }

  @Override // IntrusiveCollection.Element
  public Element getNext(IntrusiveCollection<? extends Element> list) {
    if (list != pool.getDirectiveList()) {
      return null;
    }
    return this.next;
  }

  @Override // IntrusiveCollection.Element
  public boolean isInList(IntrusiveCollection<? extends Element> list) {
    return pool == null ? false : list == pool.getDirectiveList();
  }
};
