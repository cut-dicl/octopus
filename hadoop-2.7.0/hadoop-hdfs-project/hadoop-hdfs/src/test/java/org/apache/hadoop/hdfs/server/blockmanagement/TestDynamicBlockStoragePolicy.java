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

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ReplicationVector;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.util.EnumCounters;

/**
 * @author herodotos.herodotou
 *
 */
public class TestDynamicBlockStoragePolicy {

   /**
    * @param args
    */
   @SuppressWarnings("unused")
   public static void main(String[] args) {

      Configuration conf = new Configuration();
      conf.setBoolean(DFSConfigKeys.DFS_DYNAMIC_POLICY_CONSIDER_LOAD_BALANCING_KEY, true);
      conf.setBoolean(DFSConfigKeys.DFS_DYNAMIC_POLICY_CONSIDER_PERFORMANCE_KEY, true);
      conf.setBoolean(DFSConfigKeys.DFS_DYNAMIC_POLICY_CONSIDER_DIVERSITY_KEY, true);
      conf.setBoolean(DFSConfigKeys.DFS_DYNAMIC_POLICY_CONSIDER_UTILIZATION_KEY, true);
      
      DynamicBlockStoragePolicy policy = new DynamicBlockStoragePolicy(
            (byte) 0, HdfsConstants.DYNAMIC_STORAGE_POLICY_NAME,
            new StorageType[] { StorageType.MEMORY, StorageType.SSD, StorageType.DISK }, 
            new StorageType[] { StorageType.SSD, StorageType.DISK },
            new StorageType[] { StorageType.SSD, StorageType.DISK }, 
            conf);

      ReplicationVector currRV, newRV;
      List<StorageType> types, newTypes, excessTypes;

      // Test static policy (no stats are given)
      currRV = new ReplicationVector(3);
      newRV = policy.resolveReplicationVector(currRV);
      types = policy.chooseStorageTypes(currRV.getVectorEncoding());

      newRV.incrReplication(null, (short) 5);
      newTypes = policy.chooseStorageTypes(newRV.getVectorEncoding(), types);
      types.addAll(newTypes);

      newRV.decrReplication(null, (short) 1);
      excessTypes = policy.chooseExcess(newRV.getVectorEncoding(), types);

      // Test static policy (stats are given)
      CustomDatanodeStatistics stats = createStats(1, 1, 2, 1, 1, 128, 64, 2, 600, 300);
      policy.setStats(stats);

      currRV = new ReplicationVector(3);
      newRV = policy.resolveReplicationVector(currRV);
      types = policy.chooseStorageTypes(currRV.getVectorEncoding());

      newRV.incrReplication(null, (short) 7);
      newTypes = policy.chooseStorageTypes(newRV.getVectorEncoding(), types);
      types.addAll(newTypes);

      newRV.decrReplication(null, (short) 1);
      excessTypes = policy.chooseExcess(newRV.getVectorEncoding(), types);

      // More tests
      currRV = new ReplicationVector(3);
      stats = createStats(9, 9, 4, 4, 9, 60, 60, 27, 400, 400);
      policy.setStats(stats);
      
      EnumCounters<StorageType> typeCounts = new EnumCounters<StorageType>(StorageType.class);

      for (int i = 0; i < 1000; ++i) {
         newRV = policy.resolveReplicationVector(currRV);

         System.out.println(String.format("%d\tRV: %s\tRemPc:\t%.2f\t%.2f\t%.2f", 
               i, newRV.toString(),
               stats.getCapacityRemainingPercent(StorageType.MEMORY),
               stats.getCapacityRemainingPercent(StorageType.SSD),
               stats.getCapacityRemainingPercent(StorageType.DISK)));

         for (StorageType type : StorageType.values()) {
            if (newRV.getReplication(type) > 0) {
               stats.setCapacityRemaining(type,
                     stats.getCapacityRemaining(type)
                           - newRV.getReplication(type) * 256L * 1024 * 1024);
               typeCounts.add(type, newRV.getReplication(type));
            }
         }
      }
      
      System.out.println("Type counts:");
      for (StorageType type : StorageType.values()) {
         if (typeCounts.get(type) > 0) {
            System.out.println(type + "\t" + typeCounts.get(type));
         }
      }
   }

   /**
    * 
    * @param numMemNodes
    * @param capMemGb
    * @param remMemGb
    * @param numSsdNodes
    * @param capSsdGb
    * @param remSsdGB
    * @param numDiskNodes
    * @param capDiskGb
    * @param remDiskGB
    * @return
    */
   private static CustomDatanodeStatistics createStats(int numNodes, long numMemNodes,
         long capMemGb, long remMemGb, long numSsdNodes, long capSsdGb,
         long remSsdGB, long numDiskNodes, long capDiskGb, long remDiskGB) {
      CustomDatanodeStatistics stats = new CustomDatanodeStatistics();

      stats.setNumDatanodesInService(numNodes);
      
      stats.setNumDatanodesInService(StorageType.MEMORY, (int) numMemNodes);
      stats.setCapacityTotal(StorageType.MEMORY, numMemNodes * capMemGb * 1024 * 1024 * 1024);
      stats.setCapacityRemaining(StorageType.MEMORY, numMemNodes * remMemGb * 1024 * 1024 * 1024);
      stats.setWriteThroughputSum(StorageType.MEMORY, numMemNodes * 900 * 1024);

      stats.setNumDatanodesInService(StorageType.SSD, (int) numSsdNodes);
      stats.setCapacityTotal(StorageType.SSD, numSsdNodes * capSsdGb * 1024 * 1024 * 1024);
      stats.setCapacityRemaining(StorageType.SSD, numSsdNodes * remSsdGB * 1024 * 1024 * 1024);
      stats.setWriteThroughputSum(StorageType.SSD, numSsdNodes * 300 * 1024);

      stats.setNumDatanodesInService(StorageType.DISK, (int) numDiskNodes);
      stats.setCapacityTotal(StorageType.DISK, numDiskNodes * capDiskGb * 1024 * 1024 * 1024);
      stats.setCapacityRemaining(StorageType.DISK, numDiskNodes * remDiskGB * 1024 * 1024 * 1024);
      stats.setWriteThroughputSum(StorageType.DISK, numDiskNodes * 150 * 1024);

      return stats;
   }
}
