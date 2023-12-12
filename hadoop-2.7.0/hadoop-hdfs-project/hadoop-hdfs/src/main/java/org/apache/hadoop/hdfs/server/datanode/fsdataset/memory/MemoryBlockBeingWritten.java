package org.apache.hadoop.hdfs.server.datanode.fsdataset.memory;

import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;

/**
 * This class represents replicas being written. Those are the replicas that are
 * created in a pipeline initiated by a dfs client.
 */

public class MemoryBlockBeingWritten extends MemoryBlockInPipeline {

   public MemoryBlockBeingWritten(long blockId, long genStamp, FsVolMem vol,
         long bytesToReserve, MemoryStats memoryStats) {
      super(blockId, genStamp, vol, bytesToReserve, memoryStats);
   }

   public MemoryBlockBeingWritten(long blockId, long genStamp,
         ByteBufferQueue data, ByteBufferQueue meta, FsVolMem vol,
         long bytesToReserve) {
      super(blockId, genStamp, data, meta, vol, bytesToReserve);
   }

   public MemoryBlockBeingWritten(long blockId, long len, long genStamp,
         ByteBufferQueue data, ByteBufferQueue meta, FsVolMem vol,
         Thread writer, long bytesToReserve) {
      super(blockId, len, genStamp, data, meta, vol, writer, bytesToReserve);
   }

	/**
	 * Copy constructor.
	 * 
	 * @param from
	 *            where to copy from
	 */
	public MemoryBlockBeingWritten(MemoryBlockBeingWritten from) {
		super(from);
	}

	@Override
	public long getVisibleLength() {
		return getBytesAcked(); // all acked bytes are visible
	}

	@Override // Replica
	public ReplicaState getState() {
		return ReplicaState.RBW;
	}

	@Override // Object
	public boolean equals(Object o) {
		return super.equals(o);
	}

	@Override // Object
	public int hashCode() {
		return super.hashCode();
	}

}
