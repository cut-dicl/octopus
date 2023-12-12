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
package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset.SimulatedStorage;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset.SimulatedVolume;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.DynamicVolumeChoosingPolicy;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage.State;

/**
 * @author herodotos.herodotou
 */
public class TestDynamicVolumeChoosingPolicy {

   private static Random r = new Random(1);

   /**
    * @param args
    */
   public static void main(String[] args) {
      Configuration conf = new Configuration();
      conf.setBoolean(DFSConfigKeys.DFS_DYNAMIC_POLICY_CONSIDER_LOAD_BALANCING_KEY, true);
      conf.setBoolean(DFSConfigKeys.DFS_DYNAMIC_POLICY_CONSIDER_PERFORMANCE_KEY, true);
      conf.setBoolean(DFSConfigKeys.DFS_DYNAMIC_POLICY_CONSIDER_UTILIZATION_KEY, true);

      DynamicVolumeChoosingPolicy<SimulatedVolume> policy = 
            new DynamicVolumeChoosingPolicy<SimulatedFSDataset.SimulatedVolume>();
      policy.setConf(conf);

      // Create the volumes
      int numVols = 3;
      long diskSize = 400L * 1024 * 1024 * 1024;
      int throughput = 150 * 1024;
      boolean addNoise = true;

      List<SimulatedVolume> volumes = new ArrayList<SimulatedFSDataset.SimulatedVolume>(
            numVols);
      Map<SimulatedVolume, Integer> map = new HashMap<SimulatedFSDataset.SimulatedVolume, Integer>();
      for (int i = 0; i < numVols; ++i) {
         SimulatedStorage s = new SimulatedStorage(
               diskSize + (addNoise ? r.nextInt() / 4 : 0), State.NORMAL);
         s.addBlockPool("bpid");

         SimulatedVolume v = new SimulatedVolume(s);
         v.setWriteThroughput(throughput + (addNoise ? r.nextInt(throughput / 4) : 0));
         volumes.add(v);
         map.put(v, 0);
      }

      // Test the policy
      SimulatedVolume chosen = null;
      long blockSize = 128L * 1024 * 1024;
      try {
         for (int i = 0; i < 5000; ++i) {
            chosen = policy.chooseVolume(volumes, blockSize);
            chosen.getStorage().alloc("bpid", blockSize);
            chosen.incrWriterCount();
            map.put(chosen, map.get(chosen) + 1);

            if (i % 10 == 0)
               print(i, volumes);

            for (SimulatedVolume volume : volumes) {
               while (r.nextFloat() < 0.25 && volume.getXceiverCount() > 0)
                  volume.decrWriterCount();
            }
         }
      } catch (IOException e) {
         System.out.println(e.getMessage());
      }

      System.out.print("Counts");
      for (SimulatedVolume volume : volumes) {
         System.out.print("\t" + map.get(volume));
      }
      System.out.println();
   }

   /**
    * Print stats
    * 
    * @param i
    * @param volumes
    * @throws IOException
    */
   private static void print(int i, List<SimulatedVolume> volumes)
         throws IOException {
      System.out.print(i);
      for (SimulatedVolume volume : volumes) {
         System.out.print(String.format(
               "\t%.2f\t%d",
               DFSUtil.getPercentRemaining(volume.getAvailable(),
                     volume.getCapacity()), volume.getXceiverCount()));
      }
      System.out.println();
   }
}
