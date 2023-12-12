package org.apache.hadoop.hdfs.server.datanode.fsdataset.memory;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsBlocksMetadata;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.server.datanode.CachedReplica;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.FinalizedReplica;
import org.apache.hadoop.hdfs.server.datanode.Replica;
import org.apache.hadoop.hdfs.server.datanode.ReplicaAlreadyExistsException;
import org.apache.hadoop.hdfs.server.datanode.ReplicaHandler;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInPipelineInterface;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.hdfs.server.datanode.ReplicaNotFoundException;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdfs.server.datanode.UnexpectedReplicaStateException;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaInputStreams;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaOutputStreams;
import org.apache.hadoop.hdfs.server.datanode.metrics.FSDatasetMBean;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.apache.hadoop.util.Time;

import com.google.common.base.Preconditions;

/**
 * FSDatasetMem manages a set of data blocks in memory. Each block has a unique
 * name and both its data and metadata are stored in memory.
 * 
 * It uses the {@link MemoryStore} object for synchronization.
 */
@InterfaceAudience.Private
class FsDatasetMem implements FsDatasetSpi<FsVolMem> {
   static final Log LOG = LogFactory.getLog(FsDatasetMem.class);

   private final DataNode datanode;
   private final DataStorage dataStorage;

   private final FsVolMem volume;
   private final DatanodeStorage volumeStorage;

   private final MemoryStore memoryStore;

   private boolean blockPinningEnabled;
   private ObjectName mbeanName;

   private long writeThroughput;
   private long readThroughput;
   
   /**
    * An FSDatasetMem has not a directory where it loads its data files.
    */
   FsDatasetMem(DataNode datanode, DataStorage storage, Configuration conf)
         throws IOException {
      this.datanode = datanode;
      this.dataStorage = storage;

      List<StorageLocation> dataLocations = DataNode.getStorageLocations(conf);

      this.memoryStore = new MemoryStore(conf);

      if (dataLocations.size() != 1)
         throw new IOException(
               "Expected 1 data location for FSDatasetMem, not "
                     + dataLocations.size());

      StorageLocation storageLocation = dataLocations.get(0);
      if (storageLocation.getStorageType() != StorageType.MEMORY)
         throw new IOException("Incorrect storage type for FSDatasetMem: "
               + storageLocation.getStorageType());

      Storage.StorageDirectory sd = null;
      for (int idx = 0; idx < storage.getNumStorageDirs(); idx++) {
         Storage.StorageDirectory sdInner = storage.getStorageDir(idx);
         if (storageLocation.getFile().equals(sdInner.getRoot())) {
            sd = sdInner;
         }
      }

      if (sd == null)
         throw new IOException(
               "Unable to find storage directory for FSDatasetMem");

      this.volume = new FsVolMem(this, sd.getStorageUuid(), sd.getCurrentDir(),
            StorageType.MEMORY, memoryStore);
      this.volumeStorage = volume.toDatanodeStorage();

      registerMBean(volume.getStorageType() + datanode.getDatanodeUuid());
      blockPinningEnabled = conf.getBoolean(
            DFSConfigKeys.DFS_DATANODE_BLOCK_PINNING_ENABLED,
            DFSConfigKeys.DFS_DATANODE_BLOCK_PINNING_ENABLED_DEFAULT);

      this.writeThroughput = 0l;
      this.readThroughput = 0l;
   }

   /**
    * Return the total space used by dfs datanode FSDatasetMBean
    */
   @Override
   public long getBlockPoolUsed(String bpid) throws IOException {
      return memoryStore.getBlockPoolUsed(bpid);
   }

   /**
    * Return the total space used by dfs datanode
    */
   @Override
   // FSDatasetMBean
   public long getDfsUsed() throws IOException {
      return memoryStore.getDfsUsed();
   }

   /**
    * Return total capacity, used and unused
    */
   @Override
   // FSDatasetMBean
   public long getCapacity() {
      return memoryStore.getCapacity();
   }

   /**
    * Return how many bytes can still be stored in the FSDataset
    */
   @Override
   // FSDatasetMBean
   public long getRemaining() throws IOException {
      return memoryStore.getAvailable();
   }

   @Override
   // FSDatasetMBean
   public String getStorageInfo() {
      return toString();
   }

   @Override
   // FsDatasetSpi
   public String toString() {
      return "FSDatasetMem{dirpath='" + volume.getBasePath() + "'}";
   }

   /**
    * Register the FSDataset MBean using the name
    * "hadoop:service=DataNode,name=FSDatasetState-<datanodeUuid>"
    */
   private void registerMBean(final String datanodeUuid) {
      // We wrap to bypass standard mbean naming convetion.
      // This wraping can be removed in java 6 as it is more flexible in
      // package naming for mbeans and their impl.
      try {
         StandardMBean bean = new StandardMBean(this, FSDatasetMBean.class);
         mbeanName = MBeans.register("DataNode", "FSDatasetState-"
               + datanodeUuid, bean);
      } catch (NotCompliantMBeanException e) {
         LOG.warn("Error registering FSDatasetState MBean", e);
      }
      LOG.info("Registered FSDatasetState MBean");
   }

   /**
    * Return the number of failed volumes in the FSDataset.
    */
   @Override
   // FSDatasetMBean
   public int getNumFailedVolumes() {
      return 0;
   }

   @Override
   public String[] getFailedStorageLocations() {
      return new String[0];
   }

   @Override
   public long getLastVolumeFailureDate() {
      return 0;
   }

   @Override
   public long getEstimatedCapacityLostTotal() {
      return 0;
   }

   @Override
   public long getCacheUsed() {
      return memoryStore.getCacheUsed();
   }

   @Override
   public long getCacheCapacity() {
      return memoryStore.getCacheCapacity();
   }

   @Override 
   public long getCacheAvailable() {
       return memoryStore.getCacheAvailable();
   }
   
   @Override
   public long getNumBlocksCached() {
      return 0;
   }

   @Override
   public long getNumBlocksFailedToCache() {
      return 0;
   }

   @Override
   public long getNumBlocksFailedToUncache() {
      return 0;
   }

   @Override
   public List<FsVolMem> getVolumes() {
      // return the only one volume of memory
      List<FsVolMem> volumes = new ArrayList<FsVolMem>(1);
      volumes.add(volume);
      return volumes;
   }

   @Override
   public FsVolMem addVolume(StorageLocation location,
         List<NamespaceInfo> nsInfos) throws IOException {
      throw new IOException("Cannot add a volume in FsDatasetMem");
   }

   @Override
   public List<FsVolMem> removeVolumes(Set<File> volumes, boolean clearFailure) {

      // Make sure that all volumes are absolute path.
      for (File vol : volumes) {
         Preconditions.checkArgument(vol.isAbsolute(),
               String.format("%s is not absolute path.", vol.getPath()));

         if (vol.getPath().equals(volume.getBasePath())) {
            LOG.warn("Error we can not remove the only one memory volume!!!");
         }
      }

      return new ArrayList<FsVolMem>(0);
   }

   @Override
   public DatanodeStorage getStorage(String storageUuid) {
      if (volumeStorage.getStorageID().equals(storageUuid))
         return volumeStorage;
      else
         return null;
   }

   @Override
   public StorageReport[] getStorageReports(String bpid) throws IOException {
      // memory here one storage report
      StorageReport[] reports = new StorageReport[1];

      synchronized (memoryStore) {
         reports[0] = new StorageReport(volume.toDatanodeStorage(), false,
               memoryStore.getCapacity(), memoryStore.getDfsUsed(),
               memoryStore.getAvailable(), memoryStore.getBlockPoolUsed(bpid), 
               volume.getWriteThroughput(), volume.getReadThroughput(), 
               memoryStore.getCacheCapacity(), memoryStore.getCacheUsed(),
               memoryStore.getCacheAvailable(), memoryStore.getBpCacheUsed(bpid),
               volume.getReaderCount(), volume.getWriterCount());
      }

      return reports;
   }

   @Override
   public FsVolMem getVolume(ExtendedBlock b) {
      return memoryStore.hasMemoryBlock(b.getBlockPoolId(), b.getLocalBlock()) == true ? volume
            : null;
   }

   @Override
   public Map<String, Object> getVolumeInfoMap() {
      // memory here one volume info map
      final Map<String, Object> info = new HashMap<String, Object>();
      final Map<String, Object> innerInfo = new HashMap<String, Object>();

      synchronized (memoryStore) {
         innerInfo.put("usedSpace", memoryStore.getDfsUsed());
         innerInfo.put("freeSpace", memoryStore.getAvailable());
         innerInfo.put("reservedSpace", memoryStore.getReserved());
         info.put(volume.getBasePath(), innerInfo);
      }

      return info;
   }

   @Override
   public VolumeFailureSummary getVolumeFailureSummary() {
      return null;
   }

   @Override
   public List<FinalizedReplica> getFinalizedBlocks(String bpid) {
      return null;
   }

   @Override
   public List<FinalizedReplica> getFinalizedBlocksOnPersistentStorage(
         String bpid) {
      return null;
   }

   @Override
   public void checkAndUpdate(String bpid, long blockId, File diskFile,
         File diskMetaFile, FsVolumeSpi vol, boolean finalizedORcached) throws IOException {
      throw new IOException("Memory volume - not supported!!!");
   }

   @Override
   public LengthInputStream getMetaDataInputStream(ExtendedBlock b)
         throws IOException {

      LengthInputStream in = null;

      synchronized (memoryStore) {
         MemoryBlock block = memoryStore.getMemoryBlock(b.getBlockPoolId(),
               b.getLocalBlock());
         if (block != null) {
            in = new LengthInputStream(block.getMetaInputStream(), block
                  .getMeta().size());
         }
      }

      return in;
   }

   @Override
   public long getLength(ExtendedBlock b) throws IOException {
      synchronized (memoryStore) {
         MemoryBlock block = getMemoryBlock(b.getBlockPoolId(), b.getBlockId());
         return block.getData().size();
      }
   }

   @Override
   public Replica getReplica(String bpid, long blockId) {
      return memoryStore.getMemoryBlock(bpid, blockId);
   }

   @Override
   public Replica getReplica(String bpid, long blockId, StorageType skip) {
      return memoryStore.getMemoryBlock(bpid, blockId);
   }

   @Override
   public String getReplicaString(String bpid, long blockId) {
      synchronized (memoryStore) {
         MemoryBlock b = memoryStore.getMemoryBlock(bpid, blockId);
         return b == null ? "null" : b.toString();
      }
   }

   @Override
   public Block getStoredBlock(String bpid, long blkid) throws IOException {
      synchronized (memoryStore) {
         MemoryBlock mBlock = memoryStore.getMemoryBlock(bpid, blkid);
         if (mBlock == null) {
            return null;
         }

         return new Block(blkid, mBlock.getBytesOnDisk(),
               mBlock.getGenerationStamp());
      }
   }

   @Override
   public InputStream getBlockInputStream(ExtendedBlock b, long seekOffset)
         throws IOException {
      synchronized (memoryStore) {
         MemoryBlock block = getMemoryBlock(b.getBlockPoolId(), b.getBlockId());

         ByteBufferQueueInputStream in = block.getDataInputStream();
         in.skip(seekOffset);

         return in;
      }
   }

   /**
    * Returns streams to the block data and metadata
    */
   @Override
   public ReplicaInputStreams getTmpInputStreams(ExtendedBlock b, long blkoff,
         long metaOffset) throws IOException {

      synchronized (memoryStore) {
         MemoryBlock block = getMemoryBlock(b);

         FsVolumeReference ref = block.getVolume().obtainReference();

         ByteBufferQueueInputStream dataInStream = block.getDataInputStream();
         dataInStream.skip(blkoff);

         ByteBufferQueueInputStream metaInStream = block.getMetaInputStream();
         metaInStream.skip(metaOffset);

         return new ReplicaInputStreams(dataInStream, metaInStream, ref);
      }
   }

   /**
    * Get the corresponding memory block. Throws exception if not found. Also
    * checks the generation stamp.
    * 
    * @param b
    * @return
    * @throws ReplicaNotFoundException
    */
   private MemoryBlock getMemoryBlock(ExtendedBlock b)
         throws ReplicaNotFoundException {
      MemoryBlock mBlock = memoryStore.getMemoryBlock(b.getBlockPoolId(),
            b.getLocalBlock());
      if (mBlock == null) {
         throw new ReplicaNotFoundException(
               ReplicaNotFoundException.NON_EXISTENT_REPLICA + b);
      }

      return mBlock;
   }

   /**
    * Get the corresponding memory block. Throws exception if not found. Also
    * checks the generation stamp.
    * 
    * @param bpid
    * @param blkid
    * @return
    * @throws ReplicaNotFoundException
    */
   private MemoryBlock getMemoryBlock(String bpid, long blkid)
         throws ReplicaNotFoundException {
      MemoryBlock mBlock = memoryStore.getMemoryBlock(bpid, blkid);
      if (mBlock == null) {
         throw new ReplicaNotFoundException(
               ReplicaNotFoundException.NON_EXISTENT_REPLICA + bpid + ":"
                     + blkid);
      }

      return mBlock;
   }

   @Override
   public ReplicaHandler createTemporary(String volumeId, StorageType storageType,
         ExtendedBlock b) throws IOException {

      //Check if the volume send is valid if it is not null
      if (volumeId != null && !volumeId.equals(volume.getStorageID()))
         throw new IOException("The volume ID for the memory dataset is invalid!");
      if (storageType != StorageType.MEMORY)
          throw new IOException("The memory dataset expects only memory storage type");
       
      long startTimeMs = Time.monotonicNow();
      long writerStopTimeoutMs = datanode.getDnConf().getXceiverStopTimeout();

      MemoryBlock lastFoundReplicaInfo = null;
      do {
         synchronized (memoryStore) {
            MemoryBlock currentReplicaInfo = memoryStore.getMemoryBlock(
                  b.getBlockPoolId(), b.getBlockId());

            if (currentReplicaInfo == lastFoundReplicaInfo) {
               if (lastFoundReplicaInfo != null) {
                  StorageType[] storage = {storageType};
                  invalidate(b.getBlockPoolId(),
                        new Block[] { lastFoundReplicaInfo }, storage);
               }

               if (b.getNumBytes() > memoryStore.getAvailable())
                  throw new DiskOutOfSpaceException("No more available volumes");

               FsVolumeReference ref = volume.obtainReference();

               MemoryBlockInPipeline newReplicaInfo = new MemoryBlockInPipeline(
                     b.getBlockId(), b.getGenerationStamp(), volume, 0,
                     memoryStore.getBpMemoryStats(b.getBlockPoolId()));

               memoryStore.addMemoryBlock(b.getBlockPoolId(), newReplicaInfo);

               return new ReplicaHandler(newReplicaInfo, ref);
            } else {
               if (!(currentReplicaInfo.getGenerationStamp() < b
                     .getGenerationStamp() && currentReplicaInfo instanceof MemoryBlockInPipeline)) {
                  throw new ReplicaAlreadyExistsException("Block " + b
                        + " already exists in state "
                        + currentReplicaInfo.getState()
                        + " and thus cannot be created.");
               }
               lastFoundReplicaInfo = currentReplicaInfo;
            }
         }

         // Hang too long, just bail out. This is not supposed to happen.
         long writerStopMs = Time.monotonicNow() - startTimeMs;
         if (writerStopMs > writerStopTimeoutMs) {
            LOG.warn("Unable to stop existing writer for block " + b
                  + " after " + writerStopMs + " miniseconds.");
            throw new IOException("Unable to stop existing writer for block "
                  + b + " after " + writerStopMs + " miniseconds.");
         }

         // Stop the previous writer
         ((MemoryBlockInPipeline) lastFoundReplicaInfo)
               .stopWriter(writerStopTimeoutMs);
      } while (true);
   }

   @Override
   public ReplicaHandler createRbw(StorageType storageType, ExtendedBlock b,
         boolean allowLazyPersist) throws IOException {

      synchronized (memoryStore) {
         MemoryBlock replicaInfo = memoryStore.getMemoryBlock(
               b.getBlockPoolId(), b.getBlockId());
         if (replicaInfo != null) {
            throw new ReplicaAlreadyExistsException("Block " + b
                  + " already exists in state " + replicaInfo.getState()
                  + " and thus cannot be created.");
         }

         // Check if there is available space
         long bytesToReserve = b.getLocalBlock().getNumBytes();

         if (memoryStore.getAvailable() < bytesToReserve) {
            throw new DiskOutOfSpaceException(
                  "Insufficient space for creating the " + replicaInfo);
         }

         // create a new block
         FsVolumeReference ref = volume.obtainReference();

         // create an rbw file to hold block in the designated volume
         MemoryBlockBeingWritten newReplicaInfo = new MemoryBlockBeingWritten(
               b.getBlockId(), b.getGenerationStamp(), volume, b.getNumBytes(),
               memoryStore.getBpMemoryStats(b.getBlockPoolId()));

         memoryStore.reserveSpaceForRbw(bytesToReserve);
         memoryStore.addMemoryBlock(b.getBlockPoolId(), newReplicaInfo);

         return new ReplicaHandler(newReplicaInfo, ref);
      }
   }

   @Override
   public ReplicaHandler recoverRbw(ExtendedBlock b, long newGS,
         long minBytesRcvd, long maxBytesRcvd) throws IOException {
      LOG.info("Recover RBW replica " + b);

      synchronized (memoryStore) {
         MemoryBlock replicaInfo = getMemoryBlock(b.getBlockPoolId(),
               b.getBlockId());

         // check the replica's state
         if (replicaInfo.getState() != ReplicaState.RBW) {
            throw new ReplicaNotFoundException(
                  ReplicaNotFoundException.NON_RBW_REPLICA + replicaInfo);
         }

         MemoryBlockInPipeline rbw = (MemoryBlockInPipeline) replicaInfo;

         LOG.info("Recovering " + rbw);

         // Stop the previous writer
         rbw.stopWriter(datanode.getDnConf().getXceiverStopTimeout());
         rbw.setWriter(Thread.currentThread());

         // check generation stamp
         long replicaGenerationStamp = rbw.getGenerationStamp();
         if (replicaGenerationStamp < b.getGenerationStamp()
               || replicaGenerationStamp > newGS) {
            throw new ReplicaNotFoundException(
                  ReplicaNotFoundException.UNEXPECTED_GS_REPLICA + b
                        + ". Expected GS range is [" + b.getGenerationStamp()
                        + ", " + newGS + "].");
         }

         // check replica length
         long bytesAcked = rbw.getBytesAcked();
         long numBytes = rbw.getNumBytes();
         if (bytesAcked < minBytesRcvd || numBytes > maxBytesRcvd) {
            throw new ReplicaNotFoundException("Unmatched length replica "
                  + replicaInfo + ": BytesAcked = " + bytesAcked
                  + " BytesRcvd = " + numBytes + " are not in the range of ["
                  + minBytesRcvd + ", " + maxBytesRcvd + "].");
         }

         FsVolumeReference ref = rbw.getVolume().obtainReference();
         try {
            // Truncate the potentially corrupt portion.
            // If the source was client and the last node in the pipeline was
            // lost, any corrupt data written after the acked length can go
            // unnoticed.
            if (numBytes > bytesAcked) {
               truncateBlock(memoryStore, rbw.getData(), rbw.getMeta(),
                     numBytes, bytesAcked);
               rbw.setNumBytes(bytesAcked);
               rbw.setLastChecksumAndDataLen(bytesAcked, null);
            }

            // bump the replica's generation stamp to newGS
            rbw.setGenerationStamp(newGS);

         } catch (IOException e) {
            IOUtils.cleanup(null, ref);
            throw e;
         }

         return new ReplicaHandler(rbw, ref);
      }
   }

   static private void truncateBlock(MemoryStore store, ByteBufferQueue block,
         ByteBufferQueue meta, long oldlen, long newlen) throws IOException {

      LOG.info("truncateBlock: memory block =" + block + ", oldlen=" + oldlen
            + ", newlen=" + newlen);

      if (newlen == oldlen) {
         return;
      }
      if (newlen > oldlen) {
         throw new IOException("Cannot truncate block to from oldlen (="
               + oldlen + ") to newlen (=" + newlen + ")");
      }

      DataChecksum dcs = BlockMetadataHeader.readHeader(
            new DataInputStream(new ByteBufferQueueInputStream(meta)))
            .getChecksum();
      int checksumsize = dcs.getChecksumSize();
      int bpc = dcs.getBytesPerChecksum();
      long n = (newlen - 1) / bpc + 1;
      long newmetalen = BlockMetadataHeader.getHeaderSize() + n * checksumsize;
      long lastchunkoffset = (n - 1) * bpc;
      int lastchunksize = (int) (newlen - lastchunkoffset);
      byte[] b = new byte[Math.max(lastchunksize, checksumsize)];

      // truncate block
      ByteBufferQueueOutputStream out = new ByteBufferQueueOutputStream(
            store.getDataPool(), block);
      try {
         out.setPosition(newlen);
      } finally {
         out.close();
      }

      // read last chunk
      ByteBufferQueueInputStream in = new ByteBufferQueueInputStream(block);
      try {
         in.skip(lastchunkoffset);
         in.read(b, 0, lastchunksize);
      } finally {
         in.close();
      }

      // compute checksum
      dcs.update(b, 0, lastchunksize);
      dcs.writeValue(b, 0, false);

      // update metaFile
      ByteBufferQueueOutputStream outMeta = new ByteBufferQueueOutputStream(
            store.getMetaPool(), meta);
      try {
         outMeta.setPosition(newmetalen - checksumsize);
         outMeta.write(b, 0, checksumsize);
      } finally {
         outMeta.close();
      }
   }

   @Override
   public ReplicaInPipelineInterface convertTemporaryToRbw(ExtendedBlock b)
         throws IOException {
      final long blockId = b.getBlockId();
      final long expectedGs = b.getGenerationStamp();
      final long visible = b.getNumBytes();
      LOG.info("Convert " + b + " from Temporary to RBW, visible length="
            + visible);

      synchronized (memoryStore) {
         final MemoryBlockInPipeline temp;
         {
            // get replica
            final MemoryBlock r = getMemoryBlock(b.getBlockPoolId(), blockId);

            // check the replica's state
            if (r.getState() != ReplicaState.TEMPORARY) {
               throw new ReplicaAlreadyExistsException(
                     "r.getState() != ReplicaState.TEMPORARY, r=" + r);
            }
            temp = (MemoryBlockInPipeline) r;
         }

         // check generation stamp
         if (temp.getGenerationStamp() != expectedGs) {
            throw new ReplicaAlreadyExistsException(
                  "temp.getGenerationStamp() != expectedGs = " + expectedGs
                        + ", temp=" + temp);
         }

         // check length
         final long numBytes = temp.getNumBytes();
         if (numBytes < visible) {
            throw new IOException(numBytes + " = numBytes < visible = "
                  + visible + ", temp=" + temp);
         }
         // check volume
         final FsVolMem v = (FsVolMem) temp.getVolume();
         if (v == null) {
            throw new IOException("r.getVolume() = null, temp=" + temp);
         }

         final MemoryBlockBeingWritten rbw = new MemoryBlockBeingWritten(
               blockId, numBytes, expectedGs, temp.getData(), temp.getMeta(),
               v, Thread.currentThread(), 0);
         rbw.setBytesAcked(visible);
         // overwrite the RBW in the volume map
         memoryStore.addMemoryBlock(b.getBlockPoolId(), rbw);
         return rbw;
      }
   }

   @Override
   public ReplicaHandler append(StorageType type, ExtendedBlock b, long newGS,
         long expectedBlockLen) throws IOException {
      // If the block was successfully finalized because all packets
      // were successfully processed at the Datanode but the ack for
      // some of the packets were not received by the client. The client
      // re-opens the connection and retries sending those packets.
      // The other reason is that an "append" is occurring to this block.

      // check the validity of the parameter
      if (newGS < b.getGenerationStamp()) {
         throw new IOException("The new generation stamp " + newGS
               + " should be greater than the replica " + b
               + "'s generation stamp");
      }

      synchronized (memoryStore) {
         MemoryBlock replicaInfo = getMemoryBlock(b);

         LOG.info("Appending to " + replicaInfo);
         if (replicaInfo.getState() != ReplicaState.FINALIZED) {
            throw new ReplicaNotFoundException(
                  ReplicaNotFoundException.UNFINALIZED_REPLICA + b);
         }
         if (replicaInfo.getNumBytes() != expectedBlockLen) {
            throw new IOException("Corrupted replica " + replicaInfo
                  + " with a length of " + replicaInfo.getNumBytes()
                  + " expected length is " + expectedBlockLen);
         }

         FsVolumeReference ref = replicaInfo.getVolume().obtainReference();
         MemoryBlockInPipeline replica = null;
         try {
            replica = append(b.getBlockPoolId(), replicaInfo, newGS,
                  b.getNumBytes());
         } catch (IOException e) {
            IOUtils.cleanup(null, ref);
            throw e;
         }

         return new ReplicaHandler(replica, ref);
      }
   }

   /**
    * Append to a finalized replica Change a finalized replica to be a RBW
    * replica and bump its generation stamp to be the newGS
    * 
    * @param bpid
    * @param replicaInfo
    * @param newGS
    * @param estimateBlockLen
    * @return
    * @throws IOException
    */
   private MemoryBlockBeingWritten append(String bpid, MemoryBlock replicaInfo,
         long newGS, long estimateBlockLen) throws IOException {

      if (replicaInfo.getVolume() != volume)
         throw new IOException("Unexpected data volume in FsDatasetMet");

      // construct a RBW replica with the new GS
      if (memoryStore.getAvailable() < estimateBlockLen
            - replicaInfo.getNumBytes()) {
         throw new DiskOutOfSpaceException(
               "Insufficient space for appending to " + replicaInfo);
      }

      MemoryBlockBeingWritten newReplicaInfo = new MemoryBlockBeingWritten(
            replicaInfo.getBlockId(), replicaInfo.getNumBytes(), newGS,
            replicaInfo.getData(), replicaInfo.getMeta(), volume,
            Thread.currentThread(), estimateBlockLen);

      // Replace finalized replica by a RBW replica in replicas map
      memoryStore.reserveSpaceForRbw(estimateBlockLen
            - replicaInfo.getNumBytes());
      memoryStore.addMemoryBlock(bpid, newReplicaInfo);

      return newReplicaInfo;
   }

   @Override
   public ReplicaHandler recoverAppend(ExtendedBlock b, long newGS,
         long expectedBlockLen) throws IOException {
      LOG.info("Recover failed append to " + b);

      synchronized (memoryStore) {
         MemoryBlock replicaInfo = recoverCheck(b, newGS, expectedBlockLen);

         FsVolumeReference ref = replicaInfo.getVolume().obtainReference();
         MemoryBlockInPipeline replica;

         try {
            // change the replica's state/gs etc.
            if (replicaInfo.getState() == ReplicaState.FINALIZED) {
               replica = append(b.getBlockPoolId(), replicaInfo, newGS,
                     b.getNumBytes());
            } else { // RBW
               replicaInfo.setGenerationStamp(newGS);
               replica = (MemoryBlockBeingWritten) replicaInfo;
            }
         } catch (IOException e) {
            IOUtils.cleanup(null, ref);
            throw e;
         }

         return new ReplicaHandler(replica, ref);
      }
   }

   private MemoryBlock recoverCheck(ExtendedBlock b, long newGS,
         long expectedBlockLen) throws IOException {
      MemoryBlock replicaInfo = getMemoryBlock(b.getBlockPoolId(),
            b.getBlockId());

      // check state
      if (replicaInfo.getState() != ReplicaState.FINALIZED
            && replicaInfo.getState() != ReplicaState.RBW) {
         throw new ReplicaNotFoundException(
               ReplicaNotFoundException.UNFINALIZED_AND_NONRBW_REPLICA
                     + replicaInfo);
      }

      // check generation stamp
      long replicaGenerationStamp = replicaInfo.getGenerationStamp();
      if (replicaGenerationStamp < b.getGenerationStamp()
            || replicaGenerationStamp > newGS) {
         throw new ReplicaNotFoundException(
               ReplicaNotFoundException.UNEXPECTED_GS_REPLICA
                     + replicaGenerationStamp + ". Expected GS range is ["
                     + b.getGenerationStamp() + ", " + newGS + "].");
      }

      // stop the previous writer before check a replica's length
      long replicaLen = replicaInfo.getNumBytes();
      if (replicaInfo.getState() == ReplicaState.RBW) {
         MemoryBlockInPipeline rbw = (MemoryBlockInPipeline) replicaInfo;

         // kill the previous writer
         rbw.stopWriter(datanode.getDnConf().getXceiverStopTimeout());
         rbw.setWriter(Thread.currentThread());

         // check length: bytesOnDisk and bytesAcked should be the same
         if (replicaLen != rbw.getBytesOnDisk()
               || replicaLen != rbw.getBytesAcked()) {
            throw new ReplicaAlreadyExistsException("RBW replica "
                  + replicaInfo + "bytesRcvd(" + rbw.getNumBytes()
                  + "), bytesOnDisk(" + rbw.getBytesOnDisk()
                  + "), and bytesAcked(" + rbw.getBytesAcked()
                  + ") are not the same.");
         }
      }

      // check block length
      if (replicaLen != expectedBlockLen) {
         throw new IOException("Corrupted replica " + replicaInfo
               + " with a length of " + replicaLen + " expected length is "
               + expectedBlockLen);
      }

      return replicaInfo;
   }

   @Override
   public String recoverClose(ExtendedBlock b, long newGS, long expectedBlockLen)
         throws IOException {
      LOG.info("Recover failed close " + b);

      synchronized (memoryStore) {
         // check replica's state
         MemoryBlock replicaInfo = recoverCheck(b, newGS, expectedBlockLen);
         // bump the replica's GS
         replicaInfo.setGenerationStamp(newGS);

         // finalize the replica if RBW
         if (replicaInfo.getState() == ReplicaState.RBW) {
            finalizeBlock(b.getBlockPoolId(), replicaInfo);
         }

         return replicaInfo.getStorageUuid();
      }
   }

   @Override
   public void finalizeBlock(ExtendedBlock b) throws IOException {
      if (Thread.interrupted()) {
         // Don't allow data modifications from interrupted threads
         throw new IOException("Cannot finalize block from Interrupted Thread");
      }

      synchronized (memoryStore) {
         MemoryBlock replicaInfo = getMemoryBlock(b);
         if (replicaInfo.getState() == ReplicaState.FINALIZED) {
            // this is legal, when recovery happens on a file that has
            // been opened for append but never modified
            return;
         }

         finalizeBlock(b.getBlockPoolId(), replicaInfo);
      }
   }

   private MemoryBlock finalizeBlock(String bpid, MemoryBlock replicaInfo)
         throws IOException {
      MemoryBlock newReplicaInfo = null;
      if (replicaInfo.getState() == ReplicaState.RUR
            && ((MemoryBlockUnderRecovery) replicaInfo).getOriginalReplica()
                  .getState() == ReplicaState.FINALIZED) {
         newReplicaInfo = ((MemoryBlockUnderRecovery) replicaInfo)
               .getOriginalReplica();
      } else {
         FsVolMem v = (FsVolMem) replicaInfo.getVolume();

         if (v == null) {
            throw new IOException("No volume for block " + replicaInfo);
         }

         memoryStore.releaseReservedSpace(replicaInfo.getBytesReserved());
         newReplicaInfo = new MemoryBlockFinalized(replicaInfo,
               replicaInfo.getData(), replicaInfo.getMeta(), v);
      }

      memoryStore.addMemoryBlock(bpid, newReplicaInfo);

      return newReplicaInfo;
   }

   /**
    * Remove the temporary block (if any)
    */
   @Override
   public void unfinalizeBlock(ExtendedBlock b) throws IOException {
      synchronized (memoryStore) {
         MemoryBlock replicaInfo = memoryStore.getMemoryBlock(
               b.getBlockPoolId(), b.getLocalBlock());

         if (replicaInfo != null
               && replicaInfo.getState() == ReplicaState.TEMPORARY) {
            // remove from memoryStore
            memoryStore
                  .removeMemoryBlock(b.getBlockPoolId(), b.getLocalBlock());
            LOG.warn("Block " + b + " unfinalized and removed. ");
         }
      }
   }

   @Override
   public Map<DatanodeStorage, BlockListAsLongs> getBlockReports(String bpid) {
      Map<DatanodeStorage, BlockListAsLongs> blockReportsMap = new HashMap<DatanodeStorage, BlockListAsLongs>();

      blockReportsMap.put(volume.toDatanodeStorage(),
            memoryStore.getBlockReport(bpid));

      return blockReportsMap;
   }

   @Override
   public Map<DatanodeStorage, List<Long>> getCacheReport(String bpid) {
       Map<DatanodeStorage, List<Long>> cacheReportsMap =
               new HashMap<DatanodeStorage, List<Long>>(1);

       cacheReportsMap.put(volumeStorage, memoryStore.getCacheReport(bpid));
       
       return cacheReportsMap;
   }

   @Override
   public boolean contains(ExtendedBlock block) {
      return memoryStore.hasMemoryBlock(block.getBlockPoolId(),
            block.getLocalBlock());
   }

   @Override
   public boolean contains(String bpid, long blockId) {
      return memoryStore.hasMemoryBlock(bpid, blockId);
   }

   @Override
   public void checkBlock(ExtendedBlock b, long minLength, ReplicaState state)
         throws ReplicaNotFoundException, UnexpectedReplicaStateException,
         FileNotFoundException, EOFException, IOException {
      final MemoryBlock replicaInfo = memoryStore.getMemoryBlock(
            b.getBlockPoolId(), b.getLocalBlock());

      if (replicaInfo == null) {
         throw new ReplicaNotFoundException(b);
      }
      if (replicaInfo.getState() != state) {
         throw new UnexpectedReplicaStateException(b, state);
      }

      long onDiskLength = replicaInfo.getData().size();
      if (onDiskLength < minLength) {
         throw new EOFException(b + "'s on-disk length " + onDiskLength
               + " is shorter than minLength " + minLength);
      }
   }

   /**
    * Check whether the given block is a valid one. valid means finalized
    */
   @Override
   public boolean isValidBlock(ExtendedBlock b) {
      return isValid(b, ReplicaState.FINALIZED);
   }

   /**
    * Check whether the given block is a valid RBW.
    */
   @Override
   public boolean isValidRbw(ExtendedBlock b) {
      return isValid(b, ReplicaState.RBW);
   }

   /**
    * Does the block exist and have the given state?
    */
   private boolean isValid(final ExtendedBlock b, final ReplicaState state) {
      try {
         checkBlock(b, 0, state);
      } catch (IOException e) {
         return false;
      }
      return true;
   }

   @Override
   public void invalidate(String bpid, Block[] invalidBlks, StorageType[] storageTypes) throws IOException {
      final List<String> errors = new ArrayList<String>();

      for (int i = 0; i < invalidBlks.length; i++) {
         synchronized (memoryStore) {
            final MemoryBlock info = memoryStore.getMemoryBlock(bpid,
                  invalidBlks[i]);
            if (info == null) {
               // It is okay if the block is not found
               LOG.info("Failed to delete replica " + invalidBlks[i]
                     + ": MemoryBlock not found.");
               continue;
            }

            if (info.getGenerationStamp() != invalidBlks[i]
                  .getGenerationStamp()) {
               errors.add("Failed to delete replica " + invalidBlks[i]
                     + ": GenerationStamp not matched, info=" + info);
               continue;
            }

            if (info.getVolume() == null) {
               errors.add("Failed to delete replica " + invalidBlks[i]
                     + ". No volume for this replica");
               continue;
            }
            //Check the validation of storage type MEMORY
            if (storageTypes[i] != StorageType.MEMORY) {
                errors.add("Failed to delete replica " + invalidBlks[i]
                      + ". Invalid storage type");
                continue;
             }
            
            MemoryBlock delBlk = memoryStore.removeMemoryBlock(bpid, invalidBlks[i]);
            if (delBlk != null) {
               if (invalidBlks[i].getNumBytes() != BlockCommand.NO_ACK) {
                  datanode.notifyNamenodeDeletedBlock(new ExtendedBlock(bpid,
                        invalidBlks[i]), volume.getStorageID());
               }

               LOG.info("Memory block deleted " + invalidBlks[i]);
            }
         }
      }

      if (!errors.isEmpty()) {
         StringBuilder b = new StringBuilder("Failed to delete ")
               .append(errors.size()).append(" (out of ")
               .append(invalidBlks.length).append(") replica(s):");
         for (int i = 0; i < errors.size(); i++) {
            b.append("\n").append(i).append(") ").append(errors.get(i));
         }
         throw new IOException(b.toString());
      }
   }

   @Override
   public void cache(String bpid, long[] blockIds) {
      // Nothing to do
   }

   @Override
   public void uncache(String bpid, long[] blockIds, StorageType type) throws IOException {
       final List<String> errors = new ArrayList<String>();

       for (int i = 0; i < blockIds.length; i++) {
          synchronized (memoryStore) {
             final MemoryBlock info = memoryStore.getMemoryBlock(bpid,
                     blockIds[i]);
             if (info == null) {
                // It is okay if the block is not found
                LOG.info("Failed to uncache block " + blockIds[i]
                      + ": MemoryBlock not found.");
                continue;
             }

             if (info.getState() != ReplicaState.CACHING && info.getState() != ReplicaState.CACHED) {
                errors.add("Failed to uncache replica " + blockIds[i]
                      + ": wrong replica state, info=" + info);
                continue;
             }

             if (info.getVolume() == null) {
                errors.add("Failed to uncache block " + blockIds[i]
                      + ". No volume for this block");
                continue;
             }

             //Check the validation of storage type MEMORY
             if (type != StorageType.MEMORY) { 
                 errors.add("Failed to uncache replica " + blockIds[i]
                       + ". Wrong volume type for this replica");
                 continue;
              }
             
             memoryStore.removeMemoryBlock(bpid, info);

             if (LOG.isDebugEnabled()) {
                LOG.debug("Memory block file deleted " + blockIds[i]);
             }
          }
       }

       if (!errors.isEmpty()) {
          StringBuilder b = new StringBuilder("Failed to delete ")
                .append(errors.size()).append(" (out of ")
                .append(blockIds.length).append(") replica(s):");
          for (int i = 0; i < errors.size(); i++) {
             b.append("\n").append(i).append(") ").append(errors.get(i));
          }
          throw new IOException(b.toString());
       }
   }

   @Override
   public boolean isCached(String bpid, long blockId) {
       final MemoryBlock info = memoryStore.getMemoryBlock(bpid, blockId);
       if (info == null)
           return false;          
       if (info.getState() == ReplicaState.CACHED)
           return true;
       
       return false;         
   }

   @Override
   public Set<File> checkDataDir() {
      // Nothing to do
      return null;
   }

   @Override
   public void shutdown() {

      if (mbeanName != null) {
         MBeans.unregister(mbeanName);
      }

      if (volume != null) {
         volume.shutdown();
      }
   }

   @Override
   public void adjustCrcChannelPosition(ExtendedBlock b,
         ReplicaOutputStreams streams, int checksumSize) throws IOException {

      ByteBufferQueueOutputStream metaOut = (ByteBufferQueueOutputStream) streams
            .getChecksumOut();

      long oldPos = metaOut.getPosition();
      long newPos = oldPos - checksumSize;
      if (LOG.isDebugEnabled()) {
         LOG.debug("Changing meta file offset of block " + b + " from "
               + oldPos + " to " + newPos);
      }
      metaOut.setPosition(newPos);
   }

   @Override
   public boolean hasEnoughResource() {
      return true;
   }

   @Override
   public long getReplicaVisibleLength(ExtendedBlock block) throws IOException {
      synchronized (memoryStore) {
         final MemoryBlock replica = getMemoryBlock(block.getBlockPoolId(),
               block.getBlockId());
         if (replica.getGenerationStamp() < block.getGenerationStamp()) {
            throw new IOException(
                  "replica.getGenerationStamp() < block.getGenerationStamp(), block="
                        + block + ", replica=" + replica);
         }
         return replica.getVisibleLength();
      }
   }

   @Override
   public ReplicaRecoveryInfo initReplicaRecovery(RecoveringBlock rBlock)
         throws IOException {
      synchronized (memoryStore) {
         return initReplicaRecovery(rBlock.getBlock().getBlockPoolId(),
               memoryStore, rBlock.getBlock().getLocalBlock(),
               rBlock.getNewGenerationStamp(), datanode.getDnConf()
                     .getXceiverStopTimeout());
      }

   }

   /** Check the files of a replica. */
   private static void checkMemoryBlockData(final MemoryBlock m)
         throws IOException {
      // check replica's data
      if (m.getBytesOnDisk() != m.getData().size()) {
         throw new IOException("Memory Data length mismatched. The length of "
               + m.getBlockId() + " is " + m.getData().size() + " but m=" + m);
      }

      // check replica's meta
      if (m.getMeta().size() == 0) {
         throw new IOException("Memory Metadata is empty, m=" + m);
      }
   }

   /** static version of {@link #initReplicaRecovery(RecoveringBlock)}. */
   private static ReplicaRecoveryInfo initReplicaRecovery(String bpid,
         MemoryStore memoryStore, Block block, long recoveryId,
         long xceiverStopTimeout) throws IOException {
      final MemoryBlock replica = memoryStore.getMemoryBlock(bpid,
            block.getBlockId());
      LOG.info("initReplicaRecovery: " + block + ", recoveryId=" + recoveryId
            + ", replica=" + replica);

      // check replica
      if (replica == null) {
         return null;
      }

      // stop writer if there is any
      if (replica instanceof MemoryBlockInPipeline) {
         final MemoryBlockInPipeline rip = (MemoryBlockInPipeline) replica;
         rip.stopWriter(xceiverStopTimeout);

         // check replica bytes on disk.
         if (rip.getBytesOnDisk() < rip.getVisibleLength()) {
            throw new IOException("THIS IS NOT SUPPOSED TO HAPPEN:"
                  + " getBytesOnDisk() < getVisibleLength(), rip=" + rip);
         }

         checkMemoryBlockData(rip);
      }

      // check generation stamp
      if (replica.getGenerationStamp() < block.getGenerationStamp()) {
         throw new IOException(
               "replica.getGenerationStamp() < block.getGenerationStamp(), block="
                     + block + ", replica=" + replica);
      }

      // check recovery id
      if (replica.getGenerationStamp() >= recoveryId) {
         throw new IOException("THIS IS NOT SUPPOSED TO HAPPEN:"
               + " replica.getGenerationStamp() >= recoveryId = " + recoveryId
               + ", block=" + block + ", replica=" + replica);
      }

      // check RUR
      final MemoryBlockUnderRecovery rur;
      if (replica.getState() == ReplicaState.RUR) {
         rur = (MemoryBlockUnderRecovery) replica;
         if (rur.getRecoveryID() >= recoveryId) {
            throw new RecoveryInProgressException(
                  "rur.getRecoveryID() >= recoveryId = " + recoveryId
                        + ", block=" + block + ", rur=" + rur);
         }
         final long oldRecoveryID = rur.getRecoveryID();
         rur.setRecoveryID(recoveryId);
         LOG.info("initReplicaRecovery: update recovery id for " + block
               + " from " + oldRecoveryID + " to " + recoveryId);
      } else {
         rur = new MemoryBlockUnderRecovery(replica, recoveryId);
         memoryStore.addMemoryBlock(bpid, rur);
         LOG.info("initReplicaRecovery: changing replica state for " + block
               + " from " + replica.getState() + " to " + rur.getState());
      }

      return rur.createInfo();
   }

   @Override
   public String updateReplicaUnderRecovery(ExtendedBlock oldBlock,
         long recoveryId, long newBlockId, long newLength) throws IOException {
      synchronized (memoryStore) {
         // get replica
         final String bpid = oldBlock.getBlockPoolId();
         final MemoryBlock replica = memoryStore.getMemoryBlock(bpid,
               oldBlock.getBlockId());
         LOG.info("updateReplica: " + oldBlock + ", recoveryId=" + recoveryId
               + ", length=" + newLength + ", replica=" + replica);

         // check replica
         if (replica == null) {
            throw new ReplicaNotFoundException(oldBlock);
         }

         // check replica state
         if (replica.getState() != ReplicaState.RUR) {
            throw new IOException("replica.getState() != " + ReplicaState.RUR
                  + ", replica=" + replica);
         }

         // check replica's byte on disk
         if (replica.getBytesOnDisk() != oldBlock.getNumBytes()) {
            throw new IOException("THIS IS NOT SUPPOSED TO HAPPEN:"
                  + " replica.getBytesOnDisk() != block.getNumBytes(), block="
                  + oldBlock + ", replica=" + replica);
         }

         checkMemoryBlockData(replica);

         // update replica
         final MemoryBlock finalized = updateReplicaUnderRecovery(
               oldBlock.getBlockPoolId(), (MemoryBlockUnderRecovery) replica,
               recoveryId, newBlockId, newLength);

         boolean copyTruncate = newBlockId != oldBlock.getBlockId();
         if (!copyTruncate) {
            assert finalized.getBlockId() == oldBlock.getBlockId()
                  && finalized.getGenerationStamp() == recoveryId
                  && finalized.getNumBytes() == newLength : "Replica information mismatched: oldBlock="
                  + oldBlock
                  + ", recoveryId="
                  + recoveryId
                  + ", newlength="
                  + newLength
                  + ", newBlockId="
                  + newBlockId
                  + ", finalized="
                  + finalized;
         } else {
            assert finalized.getBlockId() == oldBlock.getBlockId()
                  && finalized.getGenerationStamp() == oldBlock
                        .getGenerationStamp()
                  && finalized.getNumBytes() == oldBlock.getNumBytes() : "Finalized and old information mismatched: oldBlock="
                  + oldBlock
                  + ", genStamp="
                  + oldBlock.getGenerationStamp()
                  + ", len="
                  + oldBlock.getNumBytes()
                  + ", finalized="
                  + finalized;
         }

         checkMemoryBlockData(finalized);

         // return storage ID
         return volume.getStorageID();
      }

   }

   private MemoryBlock updateReplicaUnderRecovery(String bpid,
         MemoryBlockUnderRecovery rur, long recoveryId, long newBlockId,
         long newlength) throws IOException {

      // check recovery id
      if (rur.getRecoveryID() != recoveryId) {
         throw new IOException("rur.getRecoveryID() != recoveryId = "
               + recoveryId + ", rur=" + rur);
      }

      boolean copyOnTruncate = newBlockId > 0L
            && rur.getBlockId() != newBlockId;

      ByteBufferQueue block;
      ByteBufferQueue meta;

      if (!copyOnTruncate) {
         rur.setGenerationStamp(recoveryId);
         block = rur.getData();
         meta = rur.getMeta();
      } else {
         ByteBufferQueue[] copiedReplicas = copyBlocks(memoryStore, bpid,
               rur.getMeta(), rur.getData(), true);
         block = copiedReplicas[1];
         meta = copiedReplicas[0];
      }

      // update length
      if (rur.getNumBytes() < newlength) {
         throw new IOException("rur.getNumBytes() < newlength = " + newlength
               + ", rur=" + rur);
      }

      if (rur.getNumBytes() > newlength) {
         truncateBlock(memoryStore, rur.getData(), rur.getMeta(),
               rur.getNumBytes(), newlength);
         if (!copyOnTruncate) {
            // update RUR with the new length
            rur.setNumBytes(newlength);
         } else {
            // Copying block to a new block with new blockId.
            // Not truncating original block.
            MemoryBlockBeingWritten newReplicaInfo = new MemoryBlockBeingWritten(
                  newBlockId, recoveryId, block, meta, rur.getVolume(),
                  newlength);
            newReplicaInfo.setNumBytes(newlength);
            memoryStore.addMemoryBlock(bpid, newReplicaInfo);
            finalizeBlock(bpid, newReplicaInfo);
         }
      }

      // finalize the block
      return finalizeBlock(bpid, rur);
   }

   private static ByteBufferQueue[] copyBlocks(MemoryStore store, String bpid,
         ByteBufferQueue srcMeta, ByteBufferQueue srcData,
         boolean calculateChecksum) throws IOException {

      if (srcData.capacity() + srcMeta.capacity() > store.getAvailable())
         throw new DiskOutOfSpaceException(
               "No more available space in FsVolumeMem");

      // Compute or copy the metadata
      ByteBufferQueue dstMeta = null;
      if (calculateChecksum) {
         dstMeta = computeChecksum(store, bpid, srcMeta, srcData);
      } else {
         dstMeta = srcMeta.copy();
      }

      // Copy the block data
      ByteBufferQueue dstData = srcData.copy();

      return new ByteBufferQueue[] { dstMeta, dstData };
   }

   private static ByteBufferQueue computeChecksum(MemoryStore store,
         String bpid, ByteBufferQueue srcMeta, ByteBufferQueue block)
         throws IOException {

      final DataChecksum checksum = BlockMetadataHeader.readDataChecksum(
            new DataInputStream(new ByteBufferQueueInputStream(srcMeta)), null);
      final byte[] data = new byte[1 << 16];
      final byte[] crcs = new byte[checksum.getChecksumSize(data.length)];

      DataOutputStream metaOut = null;
      ByteBufferQueue dstMeta = new ByteBufferQueue(
            store.getBpMemoryStats(bpid));
      try {
         metaOut = new DataOutputStream(new BufferedOutputStream(
               new ByteBufferQueueOutputStream(store.getMetaPool(), dstMeta),
               HdfsConstants.SMALL_BUFFER_SIZE));

         BlockMetadataHeader.writeHeader(metaOut, checksum);

         int offset = 0;

         try (ByteBufferQueueInputStream dataIn = new ByteBufferQueueInputStream(
               block)) {

            for (int n; (n = dataIn.read(data, offset, data.length - offset)) != -1;) {
               if (n > 0) {
                  n += offset;
                  offset = n % checksum.getBytesPerChecksum();
                  final int length = n - offset;

                  if (length > 0) {
                     checksum.calculateChunkedSums(data, 0, length, crcs, 0);
                     metaOut.write(crcs, 0, checksum.getChecksumSize(length));

                     System.arraycopy(data, length, data, 0, offset);
                  }
               }
            }
         }

         // calculate and write the last crc
         checksum.calculateChunkedSums(data, 0, offset, crcs, 0);
         metaOut.write(crcs, 0, 4);
      } finally {
         IOUtils.cleanup(LOG, metaOut);
      }

      return dstMeta;
   }

   @Override
   public void addBlockPool(String bpid, Configuration conf) throws IOException {
      LOG.info("Adding block pool " + bpid);
      memoryStore.addBlockPool(bpid);
   }

   @Override
   public void shutdownBlockPool(String bpid) {
      // Delete in memory all blocks which belong to this block pool id
      LOG.info("Removing block pool " + bpid);
      memoryStore.shutdownBlockPool(bpid);

   }

   @Override
   public void deleteBlockPool(String bpid, boolean force) throws IOException {
      // Delete all blocks which belong to this block pool id
      LOG.info("Deleting block pool " + bpid);
      memoryStore.shutdownBlockPool(bpid);
   }

   @Override
   public BlockLocalPathInfo getBlockLocalPathInfo(ExtendedBlock b)
         throws IOException {
      return null;
   }

   @Override
   public HdfsBlocksMetadata getHdfsBlocksMetadata(String bpid, long[] blockIds)
         throws IOException {

      List<byte[]> blocksVolumeIds = new ArrayList<>(1);
      List<Integer> blocksVolumeIndexes = new ArrayList<Integer>(
            blockIds.length);

      blocksVolumeIds.add(ByteBuffer.allocate(4).putInt(0).array());

      for (int i = 0; i < blockIds.length; i++) {
         MemoryBlock info = memoryStore.getMemoryBlock(bpid, blockIds[i]);
         if (info != null) {
            blocksVolumeIndexes.add(0);
         } else {
            blocksVolumeIndexes.add(Integer.MAX_VALUE);
         }
      }

      return new HdfsBlocksMetadata(bpid, blockIds, blocksVolumeIds,
            blocksVolumeIndexes);
   }

   @Override
   public void enableTrash(String bpid) {
      dataStorage.enableTrash(bpid);

   }

   @Override
   public void restoreTrash(String bpid) {
      dataStorage.restoreTrash(bpid);

   }

   @Override
   public boolean trashEnabled(String bpid) {
      return dataStorage.trashEnabled(bpid);
   }

   @Override
   public void setRollingUpgradeMarker(String bpid) throws IOException {
      dataStorage.setRollingUpgradeMarker(bpid);
   }

   @Override
   public void clearRollingUpgradeMarker(String bpid) throws IOException {
      dataStorage.clearRollingUpgradeMarker(bpid);
   }

   @Override
   public void submitBackgroundSyncFileRangeRequest(ExtendedBlock block,
         FileDescriptor fd, long offset, long nbytes, int flags) {
      // nothing to do
   }

   @Override
   public void onCompleteLazyPersist(String bpId, long blockId,
         long creationTime, File[] savedFiles, FsVolMem targetVolume) {
      // nothing to do
   }

   @Override
   public void onFailLazyPersist(String bpId, long blockId) {
      // nothing to do
   }

   @Override
   public ReplicaInfo moveBlockAcrossStorage(ExtendedBlock block,
         StorageType targetStorageType) throws IOException {
      // nothing to do
      return null;
   }

   @Override
   public void setPinning(ExtendedBlock b) throws IOException {
      if (!blockPinningEnabled) {
         return;
      }

      synchronized (memoryStore) {
         MemoryBlock mBlock = getMemoryBlock(b.getBlockPoolId(), b.getBlockId());
         mBlock.setPinning();
      }
   }

   @Override
   public boolean getPinning(ExtendedBlock b) throws IOException {
      if (!blockPinningEnabled) {
         return false;
      }

      synchronized (memoryStore) {
         MemoryBlock mBlock = getMemoryBlock(b.getBlockPoolId(), b.getBlockId());
         return mBlock.getPinning();
      }
   }

   @Override
   public boolean isDeletingBlock(String bpid, long blockId) {
      return false;
   }

   @Override
   public long getWriteThroughput(){
       return writeThroughput;
   }

   @Override
   public long getReadThroughput(){
       return readThroughput;
   }

   @Override
   public void updatePerformanceMetrics() throws IOException {
       //Here we have only one volume for memory
       this.writeThroughput = volume.getWriteThroughput();
       this.readThroughput = volume.getReadThroughput();
   }

@Override
public List<CachedReplica> getCachedBlocks(String bpId) {
    return null; 
}

@Override
public ReplicaHandler createCachingBlock(String volumeId, StorageType storageType, 
        ExtendedBlock b) throws IOException {
    //Check if the volume send is valid if it is not null
    if (volumeId != null && !volumeId.equals(volume.getStorageID()))
       throw new IOException("The volume ID for the memory dataset is invalid!");
    if (storageType != StorageType.MEMORY)
        throw new IOException("The memory dataset expects only memory storage type");

    long startTimeMs = Time.monotonicNow();
    long writerStopTimeoutMs = datanode.getDnConf().getXceiverStopTimeout();

    MemoryBlock lastFoundCachingReplicaInfo = null;
    do {
       synchronized (memoryStore) {
          MemoryBlock currentCachingReplicaInfo = memoryStore.getMemoryBlock(
                b.getBlockPoolId(), b.getBlockId());

          if (currentCachingReplicaInfo == lastFoundCachingReplicaInfo) {
             if (lastFoundCachingReplicaInfo != null) {
                uncache(b.getBlockPoolId(),
                      new long[] { lastFoundCachingReplicaInfo.getBlockId() }, storageType);
             }

             if (b.getNumBytes() > memoryStore.getCacheAvailable())
                throw new DiskOutOfSpaceException("No more available cache capacity");

             FsVolumeReference ref = volume.obtainReference();

             MemoryBlockCaching newCachingReplicaInfo = new MemoryBlockCaching(
                   b.getBlockId(), b.getGenerationStamp(), volume, 0,
                   memoryStore.getBpCacheStats(b.getBlockPoolId()));

             memoryStore.addMemoryBlock(b.getBlockPoolId(), newCachingReplicaInfo);

             return new ReplicaHandler(newCachingReplicaInfo, ref);
          } else {
             if (!(currentCachingReplicaInfo.getGenerationStamp() < b
                   .getGenerationStamp() && currentCachingReplicaInfo instanceof MemoryBlockCaching)) {
                throw new ReplicaAlreadyExistsException("Block " + b
                      + " already exists in state "
                      + currentCachingReplicaInfo.getState()
                      + " and thus cannot be created.");
             }
             lastFoundCachingReplicaInfo = currentCachingReplicaInfo;
          }
       }

       // Hang too long, just bail out. This is not supposed to happen.
       long writerStopMs = Time.monotonicNow() - startTimeMs;
       if (writerStopMs > writerStopTimeoutMs) {
          LOG.warn("Unable to stop existing writer for block " + b
                + " after " + writerStopMs + " miniseconds.");
          throw new IOException("Unable to stop existing writer for block "
                + b + " after " + writerStopMs + " miniseconds.");
       }

       // Stop the previous writer
       ((MemoryBlockCaching) lastFoundCachingReplicaInfo)
             .stopWriter(writerStopTimeoutMs);
    } while (true);
 }

@Override
public void finalizeCachedBlock(ExtendedBlock b) throws IOException {
    if (Thread.interrupted()) {
        // Don't allow data modifications from interrupted threads
        throw new IOException("Cannot cache block from Interrupted Thread");
    }

    synchronized (memoryStore) {
        MemoryBlock cachedReplicaInfo = getMemoryBlock(b);
        if (cachedReplicaInfo.getState() == ReplicaState.CACHED) {
            return;
        }

        String bpid = b.getBlockPoolId();
        MemoryBlock newCachedReplicaInfo = null;


        if (cachedReplicaInfo.getState() == ReplicaState.CACHING) {

            FsVolMem v = (FsVolMem) cachedReplicaInfo.getVolume();

            if (v == null) {
                throw new IOException("No volume for block " + cachedReplicaInfo);
            }
          
            newCachedReplicaInfo = new MemoryBlockCached(cachedReplicaInfo,
                    cachedReplicaInfo.getData(), cachedReplicaInfo.getMeta(), v);

            memoryStore.addMemoryBlock(bpid, newCachedReplicaInfo);
        }

    }
}

}
