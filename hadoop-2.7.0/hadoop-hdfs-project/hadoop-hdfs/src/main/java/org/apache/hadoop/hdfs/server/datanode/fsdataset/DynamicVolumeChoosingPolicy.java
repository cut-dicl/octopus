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
package org.apache.hadoop.hdfs.server.datanode.fsdataset;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;

/**
 * A dynamic volume policy that determines which volume to use for placing a
 * block using live performance and capacity statistics.
 * 
 * @author herodotos.herodotou
 */
public class DynamicVolumeChoosingPolicy<V extends FsVolumeSpi> implements
      VolumeChoosingPolicy<V>, Configurable {

   private boolean considerLB = true;
   private boolean considerPerf = true;
   private boolean considerUtil = true;
   private Random r = new Random();

   @Override
   public V chooseVolume(List<V> volumes, long replicaSize) throws IOException {

      if (volumes.size() < 1) {
         throw new DiskOutOfSpaceException("No more available volumes");
      }

      if (considerLB == false && considerPerf == false && considerUtil == false) {
         // No metrics to consider - choose randomly
         return volumes.get(r.nextInt(volumes.size()));
      }

      // Compute necessary stats from all volumes
      long available, maxAvailable = 0L;
      double remainingPerc, maxRemainingPerc = 0d;
      double writeThroughput, maxWriteThroughput = 0d;
      int xceiverCount, minXceiverCount = 0;

      for (V volume : volumes) {
         available = volume.getAvailable();
         if (available > maxAvailable)
            maxAvailable = available;

         remainingPerc = getPercentage(available, volume.getCapacity());
         if (remainingPerc > maxRemainingPerc)
            maxRemainingPerc = remainingPerc;

         writeThroughput = volume.getWriteThroughput();
         if (writeThroughput > maxWriteThroughput)
            maxWriteThroughput = writeThroughput;

         xceiverCount = volume.getXceiverCount();
         if (xceiverCount < minXceiverCount)
            minXceiverCount = xceiverCount;
      }

      if (maxWriteThroughput > 1)
         maxWriteThroughput = Math.log(maxWriteThroughput);
      minXceiverCount += 2;

      // Evaluate all volumes and choose the best one
      V bestVolume = null;
      double currScore, bestScore = Double.MAX_VALUE;
      float c = 1;

      for (V volume : volumes) {
         double lMetric = computeLoadBalancingMetric(volume, maxRemainingPerc,
               replicaSize);
         if (lMetric < 0)
            continue; // no more space
         double pMetric = computePerformanceMetric(volume, maxWriteThroughput);
         double uMetric = computeUtilizationMetric(volume, minXceiverCount);

         currScore = Math.sqrt(lMetric * lMetric + 0.01 * pMetric * pMetric
               + uMetric * uMetric);
         if (currScore < bestScore
               || (currScore == bestScore && r.nextFloat() < 1f / ++c)) {
            bestScore = currScore;
            bestVolume = volume;
         }
      }

      if (bestVolume == null) {
         throw new DiskOutOfSpaceException("Out of space: "
               + "The volume with the most available space (=" + maxAvailable
               + " B) is less than the block size (=" + replicaSize + " B).");
      }

      return bestVolume;
   }

   /**
    * Compute the load balancing metric z(x) - f(x) based on:
    * 
    * f(x) = (rem[x] - b) / cap[x]
    * 
    * z(x) = maxRemPerc
    * 
    * Returns -1 if rem[x] < b
    * 
    * @param volume
    * @param maxRemainingPerc
    * @param replicaSize
    * @return
    * @throws IOException
    */
   private double computeLoadBalancingMetric(V volume, double maxRemainingPerc,
         long replicaSize) throws IOException {

      long available = volume.getAvailable();
      if (available < replicaSize)
         return -1d;

      if (considerLB)
         return maxRemainingPerc
               - getPercentage(volume.getAvailable() - replicaSize,
                     volume.getCapacity());
      else
         return 0d;
   }

   /**
    * Compute the performance metric z(x) - f(x) based on:
    * 
    * f(x) = log(perf[x]) / log(maxPerf)
    * 
    * z(x) = 1
    * 
    * @param volume
    * @param maxWriteThroughput
    * @return
    */
   private double computePerformanceMetric(V volume, double maxWriteThroughput) {
      if (considerPerf) {
         double result = 0d;
         if (volume.getWriteThroughput() > 1 && maxWriteThroughput > 0)
            result = Math.log(volume.getWriteThroughput()) / maxWriteThroughput;

         return 1 - result;
      } else
         return 0d;
   }

   /**
    * Compute the performance metric z(x) - f(x) based on:
    * 
    * f(x) = 1 / (util[x] + 1)
    * 
    * z(x) = 1 / (minUtil + 1)
    * 
    * @param volume
    * @param maxWriteThroughput
    * @return
    */
   private double computeUtilizationMetric(V volume, int minUtil) {
      if (considerUtil)
         return (1.0d / minUtil) - (1.0d / (volume.getXceiverCount() + 2));
      else
         return 0.0d;
   }

   /**
    * Returns the ratio remaining / capacity with appropriate error checks
    * 
    * @param remaining
    * @param capacity
    * @return
    */
   private double getPercentage(long remaining, long capacity) {
      if (remaining > 0 && capacity > 0)
         return ((double) remaining) / capacity;
      else
         return 0d;
   }

   @Override
   public void setConf(Configuration conf) {
      this.considerLB = conf.getBoolean(
            DFSConfigKeys.DFS_DYNAMIC_POLICY_CONSIDER_LOAD_BALANCING_KEY, true);
      this.considerPerf = conf.getBoolean(
            DFSConfigKeys.DFS_DYNAMIC_POLICY_CONSIDER_PERFORMANCE_KEY, true);
      this.considerUtil = conf.getBoolean(
            DFSConfigKeys.DFS_DYNAMIC_POLICY_CONSIDER_UTILIZATION_KEY, true);
   }

   @Override
   public Configuration getConf() {
      return null;
   }

}
