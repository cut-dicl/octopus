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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * This interface is used for sorting the locations of block replicas.
 * 
 * @author herodotos.herodotou
 */
@InterfaceAudience.Private
public abstract class BlockRetrievalPolicy {
   static final Log LOG = LogFactory.getLog(BlockRetrievalPolicy.class);

   /**
    * Used to setup a BlockRetrievalPolicy object.
    * 
    * @param conf
    *           the configuration object
    * @param clusterMap
    *           cluster topology
    * @param host2datanodeMap
    *           host to datanode mapping
    */
   abstract protected void initialize(Configuration conf,
         NetworkTopology clusterMap, Host2NodesMap host2datanodeMap);

   /**
    * Sort the 'nodes' array in place.
    * 
    * @param reader
    * @param nodes
    * @param storageIDs
    * @param activeLen
    */
   abstract protected void sortBlockLocations(Node reader,
         DatanodeInfo[] nodes, String[] storageIDs, int activeLen);

   /**
    * Get an instance of the configured Block Retrieval Policy based on the the
    * configuration property
    * {@link DFSConfigKeys#DFS_BLOCK_RETRIEVAL_POLICY_CLASSNAME_KEY}.
    * 
    * @param conf
    *           the configuration to be used
    * @param clusterMap
    *           the network topology of the cluster
    * @param dnManager
    *           datanode manager
    * @return an instance of BlockRetrievalPolicy
    */
   public static BlockRetrievalPolicy getInstance(Configuration conf,
         NetworkTopology clusterMap, Host2NodesMap host2datanodeMap) {
      final Class<? extends BlockRetrievalPolicy> policyClass = conf.getClass(
            DFSConfigKeys.DFS_BLOCK_RETRIEVAL_POLICY_CLASSNAME_KEY,
            DFSConfigKeys.DFS_BLOCK_RETRIEVAL_POLICY_CLASSNAME_DEFAULT,
            BlockRetrievalPolicy.class);
      final BlockRetrievalPolicy policy = ReflectionUtils.newInstance(
            policyClass, conf);
      policy.initialize(conf, clusterMap, host2datanodeMap);
      return policy;
   }

}
