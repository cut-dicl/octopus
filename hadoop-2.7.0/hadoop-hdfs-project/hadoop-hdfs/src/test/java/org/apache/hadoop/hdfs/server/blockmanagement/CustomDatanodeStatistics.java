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

import java.util.EnumMap;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSUtil;

/**
 * Class with settable global statistics for testing purposes.
 * 
 * @author herodotos.herodotou
 */
public class CustomDatanodeStatistics implements DatanodeStatistics {

   private Stats totalStats = new Stats();
   private EnumMap<StorageType, Stats> tierStats = 
         new EnumMap<StorageType, CustomDatanodeStatistics.Stats>(
         StorageType.class);

   @Override
   public long getCapacityTotal() {
      return totalStats.capacityTotal;
   }

   @Override
   public long getCapacityUsed() {
      return totalStats.capacityUsed;
   }

   @Override
   public float getCapacityUsedPercent() {
      return DFSUtil.getPercentUsed(totalStats.capacityUsed,
            totalStats.capacityTotal);
   }

   @Override
   public long getCapacityRemaining() {
      return totalStats.capacityRemaining;
   }

   @Override
   public float getCapacityRemainingPercent() {
      return DFSUtil.getPercentRemaining(totalStats.capacityRemaining,
            totalStats.capacityTotal);
   }

   @Override
   public long getBlockPoolUsed() {
      return totalStats.blockPoolUsed;
   }

   @Override
   public float getPercentBlockPoolUsed() {
      return DFSUtil.getPercentUsed(totalStats.blockPoolUsed,
            totalStats.capacityTotal);
   }

   @Override
   public long getCapacityUsedNonDFS() {
      final long nonDFSUsed = totalStats.capacityTotal
            - totalStats.capacityRemaining - totalStats.capacityUsed;
      return nonDFSUsed < 0L ? 0L : nonDFSUsed;
   }

   @Override
   public int getXceiverCount() {
      return totalStats.xceiverCount;
   }

   @Override
   public int getInServiceXceiverCount() {
      return totalStats.nodesInServiceXceiverCount;
   }

   @Override
   public int getNumDatanodesInService() {
      return totalStats.nodesInService;
   }

   @Override
   public long getCacheCapacity() {
      return totalStats.cacheCapacity;
   }

   @Override
   public long getCacheUsed() {
      return totalStats.cacheUsed;
   }

   @Override
   public long getCacheRemaining() {
      return totalStats.cacheRemaining;
   }

   @Override
   public int getExpiredHeartbeats() {
      return totalStats.expiredHeartbeats;
   }

   @Override
   public boolean containsStorageTier(StorageType type) {
      return tierStats.containsKey(type);
   }

   @Override
   public long getCapacityTotal(StorageType type) {
      Stats s = tierStats.get(type);
      return (s != null) ? s.capacityTotal : 0L;
   }

   @Override
   public long getCapacityUsed(StorageType type) {
      Stats s = tierStats.get(type);
      return (s != null) ? s.capacityUsed : 0L;
   }

   @Override
   public long getCapacityRemaining(StorageType type) {
      Stats s = tierStats.get(type);
      return (s != null) ? s.capacityRemaining : 0L;
   }

   @Override
   public float getCapacityUsedPercent(StorageType type) {
      Stats s = tierStats.get(type);
      if (s != null)
         return DFSUtil.getPercentUsed(s.capacityUsed, s.capacityTotal);
      else
         return 100f;
   }

   @Override
   public float getCapacityRemainingPercent(StorageType type) {
      Stats s = tierStats.get(type);
      if (s != null)
         return DFSUtil.getPercentRemaining(s.capacityRemaining,
               s.capacityTotal);
      else
         return 0f;
   }

   @Override
   public double getAvgWriteThroughput(StorageType type) {
      Stats s = tierStats.get(type);
      if (s != null && s.nodesInService > 0)
         return s.writeThroughputSum / (double) s.nodesInService;
      else
         return 0d;
   }

   @Override
   public double getAvgReadThroughput(StorageType type) {
      Stats s = tierStats.get(type);
      if (s != null && s.nodesInService > 0)
         return s.readThroughputSum / (double) s.nodesInService;
      else
         return 0d;
   }

   @Override
   public int getNumNodesInService(StorageType type) {
      Stats s = tierStats.get(type);
      return (s != null) ? s.nodesInService : 0;
   }

   @Override
   public long[] getStats() {
      return new long[] { getCapacityTotal(), getCapacityUsed(),
            getCapacityRemaining(), -1L, -1L, -1L, -1L };
   }

   public void setCapacityTotal(long capacityTotal) {
      totalStats.capacityTotal = capacityTotal;
   }

   public void setCapacityUsed(long capacityUsed) {
      totalStats.capacityUsed = capacityUsed;
   }

   public void setCapacityRemaining(long capacityRemaining) {
      totalStats.capacityRemaining = capacityRemaining;
   }

   public void setBlockPoolUsed(long blockPoolUsed) {
      totalStats.blockPoolUsed = blockPoolUsed;
   }

   public void setNumDatanodesInService(int nodesInService) {
      totalStats.nodesInService = nodesInService;
   }

   public void setCapacityTotal(StorageType type, long capacityTotal) {
      getTierStats(type).capacityTotal = capacityTotal;
   }

   public void setCapacityUsed(StorageType type, long capacityUsed) {
      getTierStats(type).capacityUsed = capacityUsed;
   }

   public void setCapacityRemaining(StorageType type, long capacityRemaining) {
      getTierStats(type).capacityRemaining = capacityRemaining < 0 ? 0l : capacityRemaining;
   }

   public void setBlockPoolUsed(StorageType type, long blockPoolUsed) {
      getTierStats(type).blockPoolUsed = blockPoolUsed;
   }

   public void setNumDatanodesInService(StorageType type, int nodesInService) {
      getTierStats(type).nodesInService = nodesInService;
   }

   public void setWriteThroughputSum(StorageType type, long writeThroughputSum) {
      getTierStats(type).writeThroughputSum = writeThroughputSum;
   }

   public void setReadThroughputSum(StorageType type, long readThroughputSum) {
      getTierStats(type).readThroughputSum = readThroughputSum;
   }

   private Stats getTierStats(StorageType type) {
      Stats stats = tierStats.get(type);
      if (stats == null) {
         stats = new Stats();
         tierStats.put(type, stats);
      }

      return stats;
   }

   /**
    * Private class with stats
    */
   private static class Stats {
      private long capacityTotal = 0L;
      private long capacityUsed = 0L;
      private long capacityRemaining = 0L;
      private long blockPoolUsed = 0L;
      private int xceiverCount = 0;

      private long cacheCapacity = 0L;
      private long cacheUsed = 0L;
      private long cacheRemaining = 0L;

      private int nodesInService = 0;
      private int nodesInServiceXceiverCount = 0;

      private int expiredHeartbeats = 0;

      private long writeThroughputSum = 0L;
      private long readThroughputSum = 0L;
   }
}
