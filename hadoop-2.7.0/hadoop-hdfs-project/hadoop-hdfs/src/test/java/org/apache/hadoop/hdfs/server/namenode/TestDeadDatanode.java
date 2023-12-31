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
package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.protocol.BlockReportContext;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.RegisterCommand;
import org.apache.hadoop.hdfs.server.protocol.StorageBlockReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.junit.After;
import org.junit.Test;

/**
 * Test to ensure requests from dead datnodes are rejected by namenode with
 * appropriate exceptions/failure response
 */
public class TestDeadDatanode {
  private static final Log LOG = LogFactory.getLog(TestDeadDatanode.class);
  private MiniDFSCluster cluster;

  @After
  public void cleanup() {
    cluster.shutdown();
  }

  /**
   * Test to ensure namenode rejects request from dead datanode
   * - Start a cluster
   * - Shutdown the datanode and wait for it to be marked dead at the namenode
   * - Send datanode requests to Namenode and make sure it is rejected 
   *   appropriately.
   */
  @Test
  public void testDeadDatanode() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 500);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    cluster = new MiniDFSCluster.Builder(conf).build();
    cluster.waitActive();

    String poolId = cluster.getNamesystem().getBlockPoolId();
    // wait for datanode to be marked live
    DataNode dn = cluster.getDataNodes().get(0);
    DatanodeRegistration reg = 
      DataNodeTestUtils.getDNRegistrationForBP(cluster.getDataNodes().get(0), poolId);
      
    DFSTestUtil.waitForDatanodeState(cluster, reg.getDatanodeUuid(), true, 20000);

    // Shutdown and wait for datanode to be marked dead
    dn.shutdown();
    DFSTestUtil.waitForDatanodeState(cluster, reg.getDatanodeUuid(), false, 20000);

    DatanodeProtocol dnp = cluster.getNameNodeRpc();
    
    ReceivedDeletedBlockInfo[] blocks = { new ReceivedDeletedBlockInfo(
        new Block(0), 
        ReceivedDeletedBlockInfo.BlockStatus.RECEIVED_BLOCK,
        null) };
    StorageReceivedDeletedBlocks[] storageBlocks = { 
        new StorageReceivedDeletedBlocks(reg.getDatanodeUuid(), blocks) };
    
    // Ensure blockReceived call from dead datanode is rejected with IOException
    try {
      dnp.blockReceivedAndDeleted(reg, poolId, storageBlocks);
      fail("Expected IOException is not thrown");
    } catch (IOException ex) {
      // Expected
    }

    // Ensure blockReport from dead datanode is rejected with IOException
    StorageBlockReport[] report = { new StorageBlockReport(
        new DatanodeStorage(reg.getDatanodeUuid()),
        BlockListAsLongs.EMPTY) };
    try {
      dnp.blockReport(reg, poolId, report,
          new BlockReportContext(1, 0, System.nanoTime()));
      fail("Expected IOException is not thrown");
    } catch (IOException ex) {
      // Expected
    }

    // Ensure heartbeat from dead datanode is rejected with a command
    // that asks datanode to register again
    StorageReport[] rep = { new StorageReport(new DatanodeStorage(
          reg.getDatanodeUuid()), false, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0) };

    DatanodeCommand[] cmd = dnp.sendHeartbeat(reg, rep, 0L, 0L, 0, 0, 0, null)
        .getCommands();
    assertEquals(1, cmd.length);
    assertEquals(cmd[0].getAction(), RegisterCommand.REGISTER
        .getAction());
  }
}
