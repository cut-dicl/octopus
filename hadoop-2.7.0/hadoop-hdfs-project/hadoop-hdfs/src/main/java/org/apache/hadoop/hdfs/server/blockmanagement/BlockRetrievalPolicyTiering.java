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
 * topology and storage tiers using a simple rule.
 * 
 * @author herodotos.herodotou
 */
public class BlockRetrievalPolicyTiering extends BlockRetrievalPolicy {

   private NetworkTopology clusterMap;
   private Host2NodesMap host2nodeMap;
   private boolean biasMemory;

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
      this.biasMemory = conf.getBoolean(
            DFSConfigKeys.DFS_BLOCK_RETRIEVAL_POLICY_TIERING_BIASMEM_KEY,
            DFSConfigKeys.DFS_BLOCK_RETRIEVAL_POLICY_TIERING_BIASMEM_DEFAULT);
   }

   /**
    * Sort locations based on network topology and storage tiers using a 
    * weight: the highest the tier and closest to the reader, the lower
    * the weight. Nodes with the same calculated weight are shuffled.
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
      int[] weights = new int[activeLen];
      for (int i = 0; i < activeLen; i++) {
         weights[i] = 400;
         DatanodeDescriptor dn = host2nodeMap.getDatanodeByXferAddr(
               nodes[i].getIpAddr(), nodes[i].getXferPort());
         if (dn != null) {
            DatanodeStorageInfo sdi = dn.getStorageInfo(storageIDs[i]);
            weights[i] = getWeight(reader, nodes[i], sdi);
         }
      }

      // Add weight/node pairs to a TreeMap to sort
      TreeMap<Integer, List<DatanodeInfo>> tree = new TreeMap<Integer, List<DatanodeInfo>>();
      for (int i = 0; i < activeLen; i++) {
         int weight = weights[i];
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
    * Returns a long that represents the weight from reading from that location.
    * The lower the better.
    * 
    * @param reader
    *           Node where data will be read
    * @param node
    *           Node to read from
    * @param storage
    *           Storage (on the node) to read from
    * @return weight
    */
   private int getWeight(Node reader, Node node, DatanodeStorageInfo storage) {

	  int locality = 300;
      if (reader != null) {
         if (reader.equals(node)) {
            locality = 100;
         } else if (clusterMap.isOnSameRack(reader, node)) {
            locality = 200;
         }
      }
      
      int tiering;
      if (storage != null)
         tiering = storage.getStorageType().getOrderValue();
      else
         tiering = StorageType.REMOTE.getOrderValue();
      
      int weight = locality + 10 * tiering;

      if (biasMemory && tiering <= 1) {
         // Make remote memory better than local
    	  if (locality == 200)
    		  weight = 111 + tiering;
    	  else if (locality == 300)
    		  weight = 113 + tiering;
      }
      
      return weight;
   }
}
