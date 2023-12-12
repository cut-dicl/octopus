package org.apache.hadoop.hdfs.server.datanode.fsdataset.memory;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;

public class MemoryBlockCached extends MemoryBlock {

    public MemoryBlockCached(long blockId, long len, long genStamp,
            ByteBufferQueue data, ByteBufferQueue meta, FsVolMem vol) {
        super(blockId, len, genStamp, data, meta, vol);
    }

    public MemoryBlockCached(Block block, ByteBufferQueue data,
            ByteBufferQueue meta, FsVolMem vol) {
        super(block, data, meta, vol);
    }

    public MemoryBlockCached(MemoryBlockCached from) {
        super(from);
    }
      
    @Override
    public ReplicaState getState() {
        return ReplicaState.CACHED;
    }

    @Override
    public long getVisibleLength() {
       return getNumBytes(); // all bytes are visible
    }

    @Override
    public long getBytesOnDisk() {
       return getNumBytes();
    }

     @Override  // Object
     public boolean equals(Object o) {
         return super.equals(o);
     }

     @Override  // Object
     public int hashCode() {
         return super.hashCode();
     }

     @Override
     public String toString() {
         return super.toString();
     }

}
