package org.apache.hadoop.hdfs.server.datanode;

import java.io.File;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;

public class CachingReplica extends ReplicaInPipeline {

    public CachingReplica(long blockId, long genStamp, 
            FsVolumeSpi vol, File dir, long bytesToReserve) {
      super(blockId, genStamp, vol, dir, bytesToReserve);
    }

    public CachingReplica(Block block, 
            FsVolumeSpi vol, File dir, Thread writer){
        super(block, vol, dir, writer);
    }
    
    public CachingReplica(long blockId, long len, long genStamp,
            FsVolumeSpi vol, File dir, Thread writer, long bytesToReserve){
        super(blockId, len, genStamp,vol, dir, writer, bytesToReserve);
    }
    
    public CachingReplica(CachingReplica from){
        super(from);
    }
    
    @Override  //ReplicaInfo
    public ReplicaState getState() {
      return ReplicaState.CACHING;
    }
    
}
