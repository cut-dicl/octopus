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
package org.apache.hadoop.fs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.DataChecksum;

/****************************************************
 * Provides server default configuration values to clients.
 * 
 ****************************************************/
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class FsServerDefaults implements Writable {

  static { // register a ctor
    WritableFactories.setFactory(FsServerDefaults.class, new WritableFactory() {
      @Override
      public Writable newInstance() {
        return new FsServerDefaults();
      }
    });
  }

  private long blockSize;
  private int bytesPerChecksum;
  private int writePacketSize;
  private long replicationVector;
  private int fileBufferSize;
  private boolean encryptDataTransfer;
  private long trashInterval;
  private DataChecksum.Type checksumType;

  public FsServerDefaults() {
  }

  public FsServerDefaults(long blockSize, int bytesPerChecksum,
      int writePacketSize, long replicationVector, int fileBufferSize,
      boolean encryptDataTransfer, long trashInterval,
      DataChecksum.Type checksumType) {
    this.blockSize = blockSize;
    this.bytesPerChecksum = bytesPerChecksum;
    this.writePacketSize = writePacketSize;
    this.replicationVector = replicationVector;
    this.fileBufferSize = fileBufferSize;
    this.encryptDataTransfer = encryptDataTransfer;
    this.trashInterval = trashInterval;
    this.checksumType = checksumType;
  }

  public long getBlockSize() {
    return blockSize;
  }

  public int getBytesPerChecksum() {
    return bytesPerChecksum;
  }

  public int getWritePacketSize() {
    return writePacketSize;
  }

  public short getTotalReplication() {
    return ReplicationVector.GetTotalReplication(replicationVector);
  }
  
  public long getReplicationVector() {
    return replicationVector;
  }

  public int getFileBufferSize() {
    return fileBufferSize;
  }
  
  public boolean getEncryptDataTransfer() {
    return encryptDataTransfer;
  }

  public long getTrashInterval() {
    return trashInterval;
  }

  public DataChecksum.Type getChecksumType() {
    return checksumType;
  }

  // /////////////////////////////////////////
  // Writable
  // /////////////////////////////////////////
  @Override
  @InterfaceAudience.Private
  public void write(DataOutput out) throws IOException {
    out.writeLong(blockSize);
    out.writeInt(bytesPerChecksum);
    out.writeInt(writePacketSize);
    out.writeLong(replicationVector);
    out.writeInt(fileBufferSize);
    WritableUtils.writeEnum(out, checksumType);
  }

  @Override
  @InterfaceAudience.Private
  public void readFields(DataInput in) throws IOException {
    blockSize = in.readLong();
    bytesPerChecksum = in.readInt();
    writePacketSize = in.readInt();
    replicationVector = in.readLong();
    fileBufferSize = in.readInt();
    checksumType = WritableUtils.readEnum(in, DataChecksum.Type.class);
  }
}
