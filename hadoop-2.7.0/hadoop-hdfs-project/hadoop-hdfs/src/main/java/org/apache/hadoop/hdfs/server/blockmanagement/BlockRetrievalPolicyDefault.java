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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;

/**
 * A BlockRetrievalPolicy that sorts the block locations based on network
 * topology
 * 
 * @author herodotos.herodotou
 */
public class BlockRetrievalPolicyDefault extends BlockRetrievalPolicy {

   private NetworkTopology clusterMap;

   @Override
   protected void initialize(Configuration conf, NetworkTopology clusterMap,
         Host2NodesMap host2datanodeMap) {
      this.clusterMap = clusterMap;
   }

   /**
    * Sort nodes array by network distance to <i>reader</i>.
    * <p/>
    * In a three-level topology, a node can be either local, on the same rack,
    * or on a different rack from the reader. Sorting the nodes based on network
    * distance from the reader reduces network traffic and improves performance.
    * <p/>
    * As an additional twist, we also randomize the nodes at each network
    * distance. This helps with load balancing when there is data skew.
    * 
    * @param reader
    * @param nodes
    * @param storageIDs
    * @param activeLen
    */
   @Override
   protected void sortBlockLocations(Node reader, DatanodeInfo[] nodes,
         String[] storageIDs, int activeLen) {
      clusterMap.sortByDistance(reader, nodes, activeLen);
   }

}
