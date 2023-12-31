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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor.BlockTypePair;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo.AddBlockResult;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.junit.Test;

/**
 * This class tests that methods in DatanodeDescriptor
 */
public class TestDatanodeDescriptor {
  /**
   * Test that getInvalidateBlocks observes the maxlimit.
   */
  @Test
  public void testGetInvalidateBlocks() throws Exception {
    final int MAX_BLOCKS = 10;
    final int REMAINING_BLOCKS = 2;
    final int MAX_LIMIT = MAX_BLOCKS - REMAINING_BLOCKS;
    
    DatanodeDescriptor dd = DFSTestUtil.getLocalDatanodeDescriptor();
    ArrayList<BlockTypePair> blockList = new ArrayList<BlockTypePair>(MAX_BLOCKS);
    for (int i=0; i<MAX_BLOCKS; i++) {
      blockList.add(new BlockTypePair(
          new Block(i, 0, GenerationStamp.LAST_RESERVED_STAMP),
          StorageType.DEFAULT));
    }
    dd.addBlocksToBeInvalidated(blockList);
    BlockTypePair[] bc = dd.getInvalidateBlocks(MAX_LIMIT);
    assertEquals(bc.length, MAX_LIMIT);
    bc = dd.getInvalidateBlocks(MAX_LIMIT);
    assertEquals(bc.length, REMAINING_BLOCKS);
  }
  
  @Test
  public void testBlocksCounter() throws Exception {
    DatanodeDescriptor dd = BlockManagerTestUtil.getLocalDatanodeDescriptor(true);
    assertEquals(0, dd.numBlocks());
    BlockInfoContiguous blk = new BlockInfoContiguous(new Block(1L), (short) 1);
    BlockInfoContiguous blk1 = new BlockInfoContiguous(new Block(2L), (short) 2);
    DatanodeStorageInfo[] storages = dd.getStorageInfos();
    assertTrue(storages.length > 0);
    // add first block
    assertTrue(storages[0].addBlock(blk) == AddBlockResult.ADDED);
    assertEquals(1, dd.numBlocks());
    // remove a non-existent block
    assertFalse(storages[0].removeBlock(blk1));
    assertEquals(1, dd.numBlocks());
    // add an existent block
    assertFalse(storages[0].addBlock(blk) == AddBlockResult.ADDED);
    assertEquals(1, dd.numBlocks());
    // add second block
    assertTrue(storages[0].addBlock(blk1) == AddBlockResult.ADDED);
    assertEquals(2, dd.numBlocks());
    // remove first block
    assertTrue(storages[0].removeBlock(blk));
    assertEquals(1, dd.numBlocks());
    // remove second block
    assertTrue(storages[0].removeBlock(blk1));
    assertEquals(0, dd.numBlocks());    
  }
}
