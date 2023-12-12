package org.apache.hadoop.hdfs.server.datanode.fsdataset.memory;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.Replica;

public abstract class MemoryBlock extends Block implements Replica {

	// Data members
	private ByteBufferQueue data;
	private ByteBufferQueue meta;

	private boolean pinning;

	private FsVolMem volume; // volume where the replica belongs

	// Constructors
   public MemoryBlock(Block b, ByteBufferQueue data, ByteBufferQueue meta,
         FsVolMem volume) {
      this(b.getBlockId(), b.getNumBytes(), b.getGenerationStamp(), data, meta,
            volume);
   }

   public MemoryBlock(long blockId, long numBytes, long generationStamp,
         ByteBufferQueue data, ByteBufferQueue meta, FsVolMem volume) {
      super(blockId, numBytes, generationStamp);

      this.volume = volume;
      this.data = data;
      this.meta = meta;
      this.pinning = false;
   }

   public MemoryBlock(MemoryBlock b) {
      this(b.getBlockId(), b.getNumBytes(), b.getGenerationStamp(),
            b.getData(), b.getMeta(), b.getVolume());
      this.pinning = b.pinning;
   }

	// --------------------------------------------------------------------------------------
	@Override
	public abstract ReplicaState getState();

	@Override
	public long getBytesOnDisk() {
		return data.size();
	}

	@Override
	public long getVisibleLength() {
		return data.size();
	}

   /**
    * @return Number of bytes reserved for this replica on disk.
    */
   public long getBytesReserved() {
      return 0;
   }

	@Override
	public String getStorageUuid() {

		return volume.getStorageID();
	}

	@Override
	public boolean isOnTransientStorage() {

		return volume.isTransientStorage();
	}

	@Override // Object
	public String toString() {
		return getClass().getSimpleName() + ", " + super.toString() + ", " + getState() + "\n  getNumBytes()     = "
				+ getNumBytes() + "\n  getBytesOnMem()  = " + getBytesOnDisk() + "\n  getVisibleLength()= "
				+ getVisibleLength() + "\n  getVolume()       = " + getVolume();
	}

	// --------------------------------------------------------------------------------------

	ByteBufferQueueInputStream getDataInputStream() {
		return new ByteBufferQueueInputStream(data);
	}

	ByteBufferQueueInputStream getMetaInputStream() {
		return new ByteBufferQueueInputStream(meta);
	}

	public ByteBufferQueue getData() {
		return data;
	}

	public ByteBufferQueue getMeta() {
		return meta;
	}

	public FsVolMem getVolume() {
		return volume;
	}

	public void setPinning() {
		pinning = true;
	}

	public boolean getPinning() {
		return pinning;
	}

}
