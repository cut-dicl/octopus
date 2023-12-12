package org.apache.hadoop.hdfs.server.datanode.fsdataset.memory;

import java.io.File;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.util.CloseableReferenceCount;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

/**
 * The memory volume used to store replicas.
 */
@InterfaceAudience.Private
@VisibleForTesting
public class FsVolMem implements FsVolumeSpi {
	public static final Logger LOG = LoggerFactory.getLogger(FsVolMem.class);

	private final FsDatasetMem dataset;
	private final String storageID;
	private final StorageType storageType;
	private final File currentDir; // <StorageDirectory>/current
	private final MemoryStore memoryStore;
	private CloseableReferenceCount reference = new CloseableReferenceCount();

   private long writeThroughput;
   private long readThroughput;

	private AtomicInteger readerCount;
   private AtomicInteger writerCount;
	
	FsVolMem(FsDatasetMem dataset, String storageID, File currentDir,
			StorageType storageType, MemoryStore memoryStore) {
		this.dataset = dataset;
		this.storageID = storageID;
		this.storageType = storageType;
		this.currentDir = currentDir;
		this.memoryStore = memoryStore;

	   this.writeThroughput = 0l;
	   this.readThroughput = 0l;
		this.readerCount = new AtomicInteger(0);
		this.writerCount = new AtomicInteger(0);
	}

	/**
	 * Increase the reference count. The caller must increase the reference
	 * count before issuing IOs.
	 *
	 * @throws IOException
	 *             if the volume is already closed.
	 */
	private void reference() throws ClosedChannelException {
		this.reference.reference();
	}

	/**
	 * Decrease the reference count.
	 */
	private void unreference() {
		if (FsDatasetMem.LOG.isDebugEnabled()) {
			if (reference.getReferenceCount() <= 0) {
				FsDatasetMem.LOG.debug("Decrease reference count <= 0 on "
						+ this + Joiner.on("\n").join(Thread.currentThread().getStackTrace()));
			}
		}

		checkReference();
		this.reference.unreference();
	}

	private static class FsVolumeReferenceImpl implements FsVolumeReference {
		private final FsVolMem volume;

		FsVolumeReferenceImpl(FsVolMem volume) throws ClosedChannelException {
			this.volume = volume;
			volume.reference();
		}

		/**
		 * Decreases the reference count.
		 * 
		 * @throws IOException
		 *             it never throws IOException.
		 */
		@Override
		public void close() throws IOException {
			volume.unreference();
		}

		@Override
		public FsVolumeSpi getVolume() {
			return this.volume;
		}
	}

	@Override
	public FsVolumeReference obtainReference() throws ClosedChannelException {
		return new FsVolumeReferenceImpl(this);
	}

	private void checkReference() {
		Preconditions.checkState(reference.getReferenceCount() > 0);
	}

	@Override
	public String getStorageID() {
		return storageID;
	}

	@Override
	public String[] getBlockPoolList() {
		return memoryStore.getBlockPoolList();
	}

	@Override
	public long getCapacity() {
		return memoryStore.getCapacity();
	}

	@Override
	public long getAvailable() throws IOException {
		return memoryStore.getAvailable();
	}

	@Override
	public String getBasePath() {
		return currentDir.getParent();
	}

	@Override
	public String getPath(String bpid) throws IOException {
		// we don't have directory - Memory
		throw new IOException("Memory volume has no block pool path");
	}

	@Override
	public File getFinalizedDir(String bpid) throws IOException {
		// we don't have directory - Memory
		throw new IOException("Memory volume has no finalized directory");
	}

	@Override
	public StorageType getStorageType() {
		return storageType;
	}

	@Override
	public void reserveSpaceForRbw(long bytesToReserve) {
		memoryStore.reserveSpaceForRbw(bytesToReserve);
	}

	@Override
	public void releaseReservedSpace(long bytesToRelease) {
		memoryStore.releaseReservedSpace(bytesToRelease);
	}

	@Override
	public boolean isTransientStorage() {
		return storageType.isTransient();
	}

	@Override
	public FsDatasetSpi getDataset() {
		return dataset;
	}

	DatanodeStorage toDatanodeStorage() {
		return new DatanodeStorage(storageID, DatanodeStorage.State.NORMAL, storageType);
	}

	void shutdown() {
		memoryStore.shutdown();
	}

	public MemoryStore getMemoryStore() {
		return memoryStore;
	}

	@Override
	public BlockIterator newBlockIterator(String bpid, String name) {
		return null;
	}

	@Override
	public BlockIterator loadBlockIterator(String bpid, String name) throws IOException {
		throw new IOException("Not supported!");
	}

	@Override
	public String toString() {
		return currentDir.getAbsolutePath();
	}

	@Override
	public long getWriteThroughput() {
		return writeThroughput;
	}

	@Override
	public long getReadThroughput() {
		return readThroughput;
	}

	@Override
	public void setWriteThroughput(long writeThroughput){
		this.writeThroughput = writeThroughput; 
	}

	@Override
	public void setReadThroughput(long readThroughput){
		this.readThroughput = readThroughput; 
	}

	@Override
	public File getCachedDir(String bpid) throws IOException {
	    // we don't have directory - Memory
		throw new IOException("Memory volume has no cached directory");
	}

	@Override
	public long getCacheAvailable() {
		return memoryStore.getCacheAvailable();
	}

   @Override
   public int getXceiverCount() {
      return readerCount.get() + writerCount.get();
   }
   
   @Override
   public int getReaderCount(){
      return readerCount.get();
   }

   @Override
   public void incrReaderCount() {
      readerCount.incrementAndGet();
   }
   
   @Override
   public void decrReaderCount() {
      readerCount.decrementAndGet();
   }

   @Override
   public int getWriterCount() {
      return writerCount.get();
   }

   @Override
   public void incrWriterCount() {
      writerCount.incrementAndGet();
   }
   
   @Override
   public void decrWriterCount() {
      writerCount.decrementAndGet();
   }
   
}
