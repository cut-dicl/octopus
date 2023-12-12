package org.apache.hadoop.hdfs.server.datanode.fsdataset.memory;

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.server.datanode.ChunkChecksum;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInPipelineInterface;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaOutputStreams;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.StringUtils;

public class MemoryBlockInPipeline extends MemoryBlock implements ReplicaInPipelineInterface {

	private long bytesAcked;
	private long bytesOnMem;
	private byte[] lastChecksum;
	private Thread writer;

	/**
	 * Bytes reserved for this replica on the containing volume. Based off
	 * difference between the estimated maximum block length and the bytes
	 * already written to this block.
	 */
	private long bytesReserved;

   public MemoryBlockInPipeline(long blockId, long genStamp, FsVolMem vol,
         long bytesToReserve, MemoryStats memoryStats) {
      this(blockId, 0L, genStamp, new ByteBufferQueue(memoryStats),
            new ByteBufferQueue(memoryStats), vol, Thread.currentThread(),
            bytesToReserve);
   }

   public MemoryBlockInPipeline(long blockId, long len, long genStamp,
         ByteBufferQueue data, ByteBufferQueue meta, FsVolMem vol,
         Thread writer, long bytesToReserve) {
		super(blockId, len, genStamp, data, meta, vol);
		this.bytesAcked = len;
		this.bytesOnMem = len;
		this.writer = writer;
		this.bytesReserved = bytesToReserve;
		this.lastChecksum = null;
	}

	/**
	 * Copy constructor.
	 * 
	 * @param from
	 *            where to copy from
	 */
	public MemoryBlockInPipeline(MemoryBlockInPipeline from) {
		super(from);
		this.bytesAcked = from.getBytesAcked();
		this.bytesOnMem = from.getBytesOnDisk();
		this.writer = from.writer;
		this.bytesReserved = from.bytesReserved;
	}

   public MemoryBlockInPipeline(long blockId, long genStamp,
         ByteBufferQueue data, ByteBufferQueue meta, FsVolMem vol,
         long bytesToReserve) {
      this(blockId, 0L, genStamp, data, meta, vol, Thread.currentThread(),
            bytesToReserve);
   }

	public long getVisibleLength() {
		return -1;
	}

	public ReplicaState getState() {
		return ReplicaState.TEMPORARY;
	}

	@Override
	public long getBytesAcked() {
		return bytesAcked;
	}

	@Override
	public void setBytesAcked(long bytesAcked) {
		long newBytesAcked = bytesAcked - this.bytesAcked;
		this.bytesAcked = bytesAcked;

		// Once bytes are ACK'ed we can release equivalent space from the
		// volume's reservedForRbw count. We could have released it as soon
		// as the write-to-disk completed but that would be inefficient.
		getVolume().releaseReservedSpace(newBytesAcked);
		bytesReserved -= newBytesAcked;
	}

	@Override
	public long getBytesOnDisk() {
		return bytesOnMem;
	}

	@Override
	public long getBytesReserved() {
		return bytesReserved;
	}
	
	//////////////////////////////////

	@Override
	public void releaseAllBytesReserved() { // ReplicaInPipelineInterface
		getVolume().releaseReservedSpace(bytesReserved);
		bytesReserved = 0;
	}

	@Override
	public void setLastChecksumAndDataLen(long dataLength, byte[] lastChecksum) {
		this.bytesOnMem = dataLength;
		this.lastChecksum = lastChecksum;
	}

	@Override
	public ChunkChecksum getLastChecksumAndDataLen() {
		return new ChunkChecksum(getBytesOnDisk(), lastChecksum);
	}

	/**
	 * Set the thread that is writing to this replica
	 * 
	 * @param writer
	 *            a thread writing to this replica
	 */
	public void setWriter(Thread writer) {
		this.writer = writer;
	}

	@Override // Object
	public boolean equals(Object o) {
		return super.equals(o);
	}

	/**
	 * Interrupt the writing thread and wait until it dies
	 * 
	 * @throws IOException
	 *             the waiting is interrupted
	 */
	public void stopWriter(long xceiverStopTimeout) throws IOException {
		if (writer != null && writer != Thread.currentThread() && writer.isAlive()) {
			writer.interrupt();
			try {
				writer.join(xceiverStopTimeout);
				if (writer.isAlive()) {
					final String msg = "Join on writer thread " + writer + " timed out";
					DataNode.LOG.warn(msg + "\n" + StringUtils.getStackTrace(writer));
					throw new IOException(msg);
				}
			} catch (InterruptedException e) {
				throw new IOException("Waiting for writer thread is interrupted.");
			}
		}
	}

	@Override // Object
	public int hashCode() {
		return super.hashCode();
	}

	@Override
   public ReplicaOutputStreams createStreams(boolean isCreate,
         DataChecksum requestedChecksum) throws IOException {

		long blockMemSize = 0L;
		long crcMemSize = 0L;

		// the checksum that should actually be used -- this
		// may differ from requestedChecksum for appends.
		final DataChecksum checksum;

		DataInputStream metaData = new DataInputStream(getMetaInputStream());

		if (!isCreate) {
			// For append or recovery, we must enforce the existing checksum.
			// Also, verify that the file has correct lengths, etc.
			boolean checkedMeta = false;
			try {
				BlockMetadataHeader header = BlockMetadataHeader.readHeader(metaData);
				checksum = header.getChecksum();

				if (checksum.getBytesPerChecksum() != requestedChecksum.getBytesPerChecksum()) {
					throw new IOException("Client requested checksum " + requestedChecksum
							+ " when appending to an existing block " + "with different chunk size: " + checksum);
				}

				int bytesPerChunk = checksum.getBytesPerChecksum();
				int checksumSize = checksum.getChecksumSize();

				blockMemSize = bytesOnMem;
				crcMemSize = BlockMetadataHeader.getHeaderSize()
						+ (blockMemSize + bytesPerChunk - 1) / bytesPerChunk * checksumSize;
				
            if (blockMemSize > 0
                  && (blockMemSize > getData().size() 
                        || crcMemSize > getMeta().size())) {
					throw new IOException("Corrupted block: " + this);
				}
				checkedMeta = true;

			} finally {
				if (!checkedMeta) {
					// clean up in case of exceptions.
					IOUtils.closeStream(metaData);
				}
			}
		} else {
			// for create, we can use the requested checksum
			checksum = requestedChecksum;
		}

		ByteBufferQueueOutputStream blockOut = null;
		ByteBufferQueueOutputStream crcOut = null;
		try {
			blockOut = new ByteBufferQueueOutputStream(getVolume().getMemoryStore().getDataPool(), getData());
			crcOut = new ByteBufferQueueOutputStream(getVolume().getMemoryStore().getMetaPool(), getMeta());
			if (!isCreate) {
				blockOut.setPosition(blockMemSize);
				crcOut.setPosition(crcMemSize);
			}
			
         return new ReplicaOutputStreams(blockOut, crcOut, checksum,
               getVolume().isTransientStorage(), StorageType.MEMORY);

		} catch (Exception e) {
			IOUtils.closeStream(blockOut);
			IOUtils.closeStream(crcOut);
			throw e;
		}
	}

	// ----------------------------------------------------------------------------------------------------------

	public String toString() {
		return super.toString() 
		      + "\n  bytesAcked=" + bytesAcked 
		      + "\n  bytesOnMem=" + bytesOnMem;
	}
}
