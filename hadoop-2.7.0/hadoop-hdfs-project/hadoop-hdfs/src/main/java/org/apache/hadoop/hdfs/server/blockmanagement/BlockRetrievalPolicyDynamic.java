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
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

/**
 * A BlockRetrievalPolicy that sorts the block locations based on network
 * topology and performance statistics.
 * 
 * @author herodotos.herodotou
 */
public class BlockRetrievalPolicyDynamic extends BlockRetrievalPolicy {

   private NetworkTopology clusterMap;
   private Host2NodesMap host2nodeMap;

   private long netSpeedRack = Long.MAX_VALUE; // in KBps
   private long netSpeedCluster = Long.MAX_VALUE; // in KBps

   private boolean considerUtil = true;
   
   private static final Random r = new Random();

   @VisibleForTesting
   void setRandomSeed(long seed) {
      r.setSeed(seed);
   }

   @Override
   protected void initialize(Configuration conf, NetworkTopology clusterMap,
         Host2NodesMap host2datanodeMap) {
      this.clusterMap = clusterMap;
      this.host2nodeMap = host2datanodeMap;

      this.considerUtil = conf.getBoolean(
            DFSConfigKeys.DFS_DYNAMIC_POLICY_CONSIDER_UTILIZATION_KEY, true);
      
      this.netSpeedRack = conf.getLong(
            DFSConfigKeys.DFS_NETWORK_SPEED_RACK_MBPS_KEY,
            DFSConfigKeys.DFS_NETWORK_SPEED_MBPS_DEFAULT) * 1024 / 8;
      this.netSpeedCluster = conf.getLong(
            DFSConfigKeys.DFS_NETWORK_SPEED_CLUSTER_MBPS_KEY,
            DFSConfigKeys.DFS_NETWORK_SPEED_MBPS_DEFAULT) * 1024 / 8;

      if (netSpeedCluster >= netSpeedRack)
         netSpeedCluster = (long) (0.8 * netSpeedRack);
   }

   /**
    * Sort locations based on network topology and read throughput of the
    * storage using the formula: min(netSpeed, storageSpeed). The network speed
    * depends on network topology and the storage speed based on the type of the
    * storage media. Nodes with the same calculated speed are shuffled.
    * 
    * @param reader
    * @param nodes
    * @param storageIDs
    * @param activeLen
    */
   @Override
   protected void sortBlockLocations(Node reader, DatanodeInfo[] nodes,
         String[] storageIDs, int activeLen) {

      if (nodes.length <= 1)
         return;

      // Compute the weight for reading from each storage
      long[] weights = new long[activeLen];
      for (int i = 0; i < activeLen; i++) {
         weights[i] = 0;
         DatanodeDescriptor dn = host2nodeMap.getDatanodeByXferAddr(
               nodes[i].getIpAddr(), nodes[i].getXferPort());
         if (dn != null) {
            DatanodeStorageInfo sdi = dn.getStorageInfo(storageIDs[i]);
            if (sdi != null) {
               weights[i] = getWeight(reader, nodes[i], sdi);
            }
         }
      }

      // Add weight/node pairs to a TreeMap to sort
      TreeMap<Long, List<DatanodeInfo>> tree = new TreeMap<Long, List<DatanodeInfo>>(
            Collections.reverseOrder());
      for (int i = 0; i < activeLen; i++) {
         long weight = weights[i];
         DatanodeInfo node = nodes[i];
         List<DatanodeInfo> list = tree.get(weight);
         if (list == null) {
            list = Lists.newArrayListWithExpectedSize(1);
            tree.put(weight, list);
         }
         list.add(node);
      }

      // Place the sorted locations in the node array
      int idx = 0;
      for (List<DatanodeInfo> list : tree.values()) {
         if (list != null) {
            Collections.shuffle(list, r);
            for (DatanodeInfo node : list) {
               nodes[idx] = node;
               idx++;
            }
         }
      }

      if (LOG.isDebugEnabled())
         LOG.debug("Reader: " + reader + " Nodes: " + Arrays.toString(nodes));
   }

   /**
    * Returns a long that represents the throughput that can be achieved for
    * reading the data from the 'storage' based on min(netSpeed, storageSpeed).
    * 
    * netSpeed depends on the network topology
    * 
    * storageSpeed depends on the throughput and number of active connections:
    * storageSpeed = (throughput / 2) + (throughput / 2 * (xceiver + 1))
    * 
    * The higher the better.
    * 
    * @param reader
    *           Node where data will be read
    * @param node
    *           Node to read from
    * @param storage
    *           Storage (on the node) to read from
    * @return weight
    */
   private long getWeight(Node reader, Node node, DatanodeStorageInfo storage) {
      // Compute network speed
      long netSpeed = netSpeedCluster;
      if (reader != null) {
         if (reader.equals(node)) {
            netSpeed = Long.MAX_VALUE;
         } else if (clusterMap.isOnSameRack(reader, node)) {
            netSpeed = netSpeedRack;
         }
      }
      
      int dnXceiver = storage.getDatanodeDescriptor().getApproXceiverCount();
      netSpeed = adjustSpeed(netSpeed, dnXceiver);

      // Compute storage speed
      int storXceiver = storage.getApproXceiverCount();
      long storageSpeed = storage.getReadThroughput();
      if (storageSpeed <= 0) {
         storageSpeed = Long.MAX_VALUE;
      } else {
         storageSpeed = adjustSpeed(storageSpeed, storXceiver);
      }

      // Compute the weight
      long weight = Math.min(netSpeed, storageSpeed);
      if (weight != Long.MAX_VALUE) {
         // Introduce small bias for xceiver count
         if (considerUtil && weight == netSpeed)
            weight += 2048 / (Math.max(dnXceiver, storXceiver) + 1);

         // Introduce small bias for storage type
         weight += (StorageType.COUNT - storage.getStorageType()
               .getOrderValue()) * 512;

         // Introduce small bias for topology
         if (netSpeed == Long.MAX_VALUE)
            weight *= 20;
         else if (storage.getStorageType() != StorageType.MEMORY)
            weight /= 2;
      }

      return weight;
   }

   /**
    * Lower the speed based on the xceiver count
    * 
    * @param speed
    * @param xceiver
    * @return
    */
   private long adjustSpeed(long speed, int xceiver) {
      if (considerUtil && speed != Long.MAX_VALUE && xceiver > 0) {
         if (xceiver < 8)
            speed = (long) (speed - (speed * xceiver / 10.0d));
         else
            speed = (long) (2.0d * speed / (xceiver + 1));
      }

      return speed;
   }
}
