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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor.DatanodeDescrTypePair;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor.DatanodeDescrTypePairs;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.ipc.Server;

import java.util.*;

/**
 * Stores information about all corrupt blocks in the File System.
 * A Block is considered corrupt only if all of its replicas are
 * corrupt. While reporting replicas of a Block, we hide any corrupt
 * copies. These copies are removed once Block is found to have 
 * expected number of good replicas.
 * Mapping: Block -> TreeSet<DatanodeDescriptor> 
 */

@InterfaceAudience.Private
public class CorruptReplicasMap{

  /** The corruption reason code */
  public static enum Reason {
    NONE,                // not specified.
    ANY,                 // wildcard reason
    GENSTAMP_MISMATCH,   // mismatch in generation stamps
    SIZE_MISMATCH,       // mismatch in sizes
    INVALID_STATE,       // invalid state
    CORRUPTION_REPORTED  // client or datanode reported the corruption
  }

  private final SortedMap<Block, Map<DatanodeDescrTypePair, Reason>> corruptReplicasMap =
    new TreeMap<Block, Map<DatanodeDescrTypePair, Reason>>();

  /**
   * Mark the block belonging to datanode as corrupt.
   *
   * @param blk Block to be added to CorruptReplicasMap
   * @param dn DatanodeDescriptor which holds the corrupt replica
   * @param reason a textual reason (for logging purposes)
   * @param reasonCode the enum representation of the reason
   */
  void addToCorruptReplicasMap(Block blk, DatanodeDescriptor dn, StorageType type, 
      String reason, Reason reasonCode) {
    Map <DatanodeDescrTypePair, Reason> nodes = corruptReplicasMap.get(blk);
    if (nodes == null) {
      nodes = new HashMap<DatanodeDescrTypePair, Reason>();
      corruptReplicasMap.put(blk, nodes);
    }
    
    String reasonText;
    if (reason != null) {
      reasonText = " because " + reason;
    } else {
      reasonText = "";
    }
    
    DatanodeDescrTypePair pair = new DatanodeDescrTypePair(dn, type);
    if (!nodes.keySet().contains(pair)) {
      NameNode.blockStateChangeLog.info(
          "BLOCK NameSystem.addToCorruptReplicasMap: {} added as corrupt on "
              + "{} ({}) by {} {}", blk.getBlockName(), dn, type,
              Server.getRemoteIp(), reasonText);
    } else {
      NameNode.blockStateChangeLog.info(
          "BLOCK NameSystem.addToCorruptReplicasMap: duplicate requested for" +
              " {} to add as corrupt on {} ({}) by {} {}", blk.getBlockName(), dn,
              type, Server.getRemoteIp(), reasonText);
    }
    // Add the node or update the reason.
    nodes.put(pair, reasonCode);
  }

  /**
   * Remove Block from CorruptBlocksMap
   *
   * @param blk Block to be removed
   */
  void removeFromCorruptReplicasMap(Block blk) {
    if (corruptReplicasMap != null) {
      corruptReplicasMap.remove(blk);
    }
  }

  /**
   * Remove the block at the given datanode for ALL storage types from
   * CorruptBlockMap
   * 
   * @param blk
   *          block to be removed
   * @param datanode
   *          datanode where the block is located
   * @return true if the removal is successful; false if the replica is not in
   *         the map
   */
  boolean removeFromCorruptReplicasMap(Block blk, DatanodeDescriptor datanode) {
    boolean removed = false;

    for (StorageType type : StorageType.values()) {
      if (removeFromCorruptReplicasMap(blk, datanode, type, Reason.ANY))
        removed = true;
    }

    return removed;
  }

  /**
   * Remove the block at the given datanode and storage type from CorruptBlockMap
   * @param blk block to be removed
   * @param datanode datanode where the block is located
   * @param type storage type
   * @return true if the removal is successful; 
             false if the replica is not in the map
   */ 
  boolean removeFromCorruptReplicasMap(Block blk, DatanodeDescriptor datanode,
      StorageType type) {
    return removeFromCorruptReplicasMap(blk, datanode, type, Reason.ANY);
  }

  boolean removeFromCorruptReplicasMap(Block blk, DatanodeDescriptor datanode,
      StorageType type, Reason reason) {
    Map <DatanodeDescrTypePair, Reason> datanodes = corruptReplicasMap.get(blk);
    if (datanodes==null)
      return false;

    // if reasons can be compared but don't match, return false.
    DatanodeDescrTypePair pair = new DatanodeDescrTypePair(datanode, type);
    Reason storedReason = datanodes.get(pair);
    if (reason != Reason.ANY && storedReason != null &&
        reason != storedReason) {
      return false;
    }

    if (datanodes.remove(pair) != null) { // remove the replicas
      if (datanodes.isEmpty()) {
        // remove the block if there is no more corrupted replicas
        corruptReplicasMap.remove(blk);
      }
      return true;
    }
    return false;
  }
    

  /**
   * Get Nodes which have corrupt replicas of Block
   * 
   * @param blk Block for which nodes are requested
   * @return collection of nodes. Null if does not exists
   */
  DatanodeDescrTypePairs getNodes(Block blk) {
    Map <DatanodeDescrTypePair, Reason> nodes = corruptReplicasMap.get(blk);
    if (nodes == null)
      return null;
    return new DatanodeDescrTypePairs(nodes.keySet());
  }

  /**
   * Check if replica belonging to Datanode is corrupt
   *
   * @param blk Block to check
   * @param node DatanodeDescriptor which holds the replica
   * @return true if replica is corrupt, false if does not exists in this map
   */
  boolean isReplicaCorrupt(Block blk, DatanodeDescriptor node, StorageType type) {
    DatanodeDescrTypePairs nodes = getNodes(blk);
    return ((nodes != null) && (nodes.contains(node, type)));
  }

  int numCorruptReplicas(Block blk) {
    DatanodeDescrTypePairs nodes = getNodes(blk);
    return (nodes == null) ? 0 : nodes.count();
  }
  
  int size() {
    return corruptReplicasMap.size();
  }

  /**
   * Return a range of corrupt replica block ids. Up to numExpectedBlocks 
   * blocks starting at the next block after startingBlockId are returned
   * (fewer if numExpectedBlocks blocks are unavailable). If startingBlockId 
   * is null, up to numExpectedBlocks blocks are returned from the beginning.
   * If startingBlockId cannot be found, null is returned.
   *
   * @param numExpectedBlocks Number of block ids to return.
   *  0 <= numExpectedBlocks <= 100
   * @param startingBlockId Block id from which to start. If null, start at
   *  beginning.
   * @return Up to numExpectedBlocks blocks from startingBlockId if it exists
   *
   */
  long[] getCorruptReplicaBlockIds(int numExpectedBlocks,
                                   Long startingBlockId) {
    if (numExpectedBlocks < 0 || numExpectedBlocks > 100) {
      return null;
    }
    
    Iterator<Block> blockIt = corruptReplicasMap.keySet().iterator();
    
    // if the starting block id was specified, iterate over keys until
    // we find the matching block. If we find a matching block, break
    // to leave the iterator on the next block after the specified block. 
    if (startingBlockId != null) {
      boolean isBlockFound = false;
      while (blockIt.hasNext()) {
        Block b = blockIt.next();
        if (b.getBlockId() == startingBlockId) {
          isBlockFound = true;
          break; 
        }
      }
      
      if (!isBlockFound) {
        return null;
      }
    }

    ArrayList<Long> corruptReplicaBlockIds = new ArrayList<Long>();

    // append up to numExpectedBlocks blockIds to our list
    for(int i=0; i<numExpectedBlocks && blockIt.hasNext(); i++) {
      corruptReplicaBlockIds.add(blockIt.next().getBlockId());
    }
    
    long[] ret = new long[corruptReplicaBlockIds.size()];
    for(int i=0; i<ret.length; i++) {
      ret[i] = corruptReplicaBlockIds.get(i);
    }
    
    return ret;
  }

  /**
   * return the reason about corrupted replica for a given block
   * on a given dn
   * @param block block that has corrupted replica
   * @param node datanode that contains this corrupted replica
   * @return reason
   */
  String getCorruptReason(Block block, DatanodeDescriptor node, StorageType type) {
    Reason reason = null;
    if(corruptReplicasMap.containsKey(block)) {
      DatanodeDescrTypePair pair = new DatanodeDescrTypePair(node, type);
      if (corruptReplicasMap.get(block).containsKey(pair)) {
        reason = corruptReplicasMap.get(block).get(pair);
      }
    }
    if (reason != null) {
      return reason.toString();
    } else {
      return null;
    }
  }
}
