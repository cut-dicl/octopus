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
package org.apache.hadoop.hdfs.server.protocol;

import org.apache.hadoop.util.StringUtils;

/**
 * Utilization report for a Datanode storage
 */
public class StorageReport {
  private final DatanodeStorage storage;
  private final boolean failed;
  private final long capacity;
  private final long dfsUsed;
  private final long remaining;
  private final long blockPoolUsed;
  
  private final long cacheCapacity;
  private final long cacheUsed;
  private final long cacheRemaining;
  private final long cacheBpUsed;
  
  private final long writeThroughput;
  private final long readThroughput;
  private final int readerCount;
  private final int writerCount;
  
  public static final StorageReport[] EMPTY_ARRAY = {};
   
  public StorageReport(DatanodeStorage storage, boolean failed,
    long capacity, long dfsUsed, long remaining, long bpUsed, 
    long writeThroughput, long readThroughput, 
    long cacheCapacity, long cacheUsed, 
    long cacheRemaining, long cacheBpUsed, 
    int readerCount, int writerCount) {
      
    this.storage = storage;
    this.failed = failed;
    this.capacity = capacity;
    this.dfsUsed = dfsUsed;
    this.remaining = remaining;
    this.blockPoolUsed = bpUsed;
    this.writeThroughput = writeThroughput;
    this.readThroughput = readThroughput;
    this.cacheCapacity = cacheCapacity;
    this.cacheUsed = cacheUsed;
    this.cacheRemaining = cacheRemaining;
    this.cacheBpUsed = cacheBpUsed;
    this.readerCount = readerCount;
    this.writerCount = writerCount;
  }

  public DatanodeStorage getStorage() {
    return storage;
  }

  public boolean isFailed() {
    return failed;
  }

  public long getCapacity() {
    return capacity;
  }

  public long getDfsUsed() {
    return dfsUsed;
  }

  public long getRemaining() {
    return remaining;
  }

  public long getBlockPoolUsed() {
    return blockPoolUsed;
  }
  
  public long getWriteThroughput() {
      return writeThroughput; 
  }

  public long getReadThroughput() {
      return readThroughput; 
  }

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
   * @return Amount of cache remaining in bytes
   */
  public long getCacheRemaining() {
    return cacheRemaining;
  }

  /**
   * @return Amount of Block Pool cache used in bytes
   */
  public long getCacheBpUsed() {
    return cacheBpUsed;
  }

  public int getReaderCount() {
     return readerCount;
  }
  
  public int getWriterCount() {
     return writerCount;
  }

  public String getStorageReport() {
      StringBuilder buffer = new StringBuilder();
      long c = getCapacity();
      long r = getRemaining();
      long u = getDfsUsed();
      long cc = getCacheCapacity();
      long cr = getCacheRemaining();
      long cu = getCacheUsed();

      buffer.append("StorageID: "+ storage.getStorageID()+"\n");
      buffer.append("StorageType: "+ storage.getStorageType()+"\n");

      buffer.append("Configured Capacity: "+c+" ("+StringUtils.byteDesc(c)+")"+"\n");
      buffer.append("DFS Used: "+u+" ("+StringUtils.byteDesc(u)+")"+"\n");
      buffer.append("DFS Remaining: " +r+ " ("+StringUtils.byteDesc(r)+")"+"\n");

      buffer.append("Configured Cache Capacity: "+cc+" ("+StringUtils.byteDesc(cc)+")"+"\n");
      buffer.append("Cache Used: "+cu+" ("+StringUtils.byteDesc(cu)+")"+"\n");
      buffer.append("Cache Remaining: " +cr+ " ("+StringUtils.byteDesc(cr)+")"+"\n");

      buffer.append("Xceiver Count: "+ (readerCount + writerCount) +"\n");

      return buffer.toString();
  }
}
