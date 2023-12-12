package org.apache.hadoop.hdfs.server.datanode.fsdataset.memory;

import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;

public class MemoryBlockCaching extends MemoryBlockInPipeline{

    public MemoryBlockCaching(long blockId, long genStamp, FsVolMem vol,
            long bytesToReserve, MemoryStats memoryStats) {
        super(blockId, genStamp, vol, bytesToReserve, memoryStats);
    }

    public MemoryBlockCaching(long blockId, long len, long genStamp,
            ByteBufferQueue data, ByteBufferQueue meta, FsVolMem vol,
            Thread writer, long bytesToReserve){
        super(blockId, len, genStamp, data, meta, vol,writer, bytesToReserve);
    }
    
    public MemoryBlockCaching(long blockId, long genStamp,
            ByteBufferQueue data, ByteBufferQueue meta, FsVolMem vol,
            long bytesToReserve){
        super(blockId, genStamp, data, meta, vol, bytesToReserve);
    }
    
    public MemoryBlockCaching(MemoryBlockCaching from){
        super(from);
    }
    
    @Override
    public ReplicaState getState() {
        return ReplicaState.CACHING;
    }
    
    @Override
    ByteBufferQueueInputStream getDataInputStream() {
       return new ByteBufferQueueInputStream(getData(), true);
    }

    @Override
    ByteBufferQueueInputStream getMetaInputStream() {
       return new ByteBufferQueueInputStream(getMeta(), true);
    }
}
