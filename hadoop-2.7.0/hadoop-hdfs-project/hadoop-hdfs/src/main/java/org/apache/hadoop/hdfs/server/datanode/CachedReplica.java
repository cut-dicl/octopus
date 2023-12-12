package org.apache.hadoop.hdfs.server.datanode;

import java.io.File;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;

public class CachedReplica extends ReplicaInfo {

    public CachedReplica(long blockId, long len, long genStamp,
            FsVolumeSpi vol, File dir) {
        super(blockId, len, genStamp, vol, dir);
    }

    public CachedReplica(Block block, FsVolumeSpi vol, File dir) {
        super(block, vol, dir);
    }

    public CachedReplica(CachedReplica from) {
        super(from);
    }
    
    @Override
    public ReplicaState getState() {
        return ReplicaState.CACHED;
    }

    @Override
    public long getBytesOnDisk() {
        return getNumBytes();
    }

    @Override
    public long getVisibleLength() {
        return getNumBytes();       // all bytes are visible
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
