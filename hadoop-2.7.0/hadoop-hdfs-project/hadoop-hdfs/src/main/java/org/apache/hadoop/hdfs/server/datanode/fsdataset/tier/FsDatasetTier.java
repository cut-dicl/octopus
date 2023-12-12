/**
 * 
 * This is a service provider interface for the underlying storage (various tiers) that
 * stores replicas for a data node.
 * This implementation stores replicas on local drives, memory, ssd, remote storage. 
 * 
 **/

package org.apache.hadoop.hdfs.server.datanode.fsdataset.tier;

import java.io.EOFException;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsBlocksMetadata;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.CachedReplica;
import org.apache.hadoop.hdfs.server.datanode.FinalizedReplica;
import org.apache.hadoop.hdfs.server.datanode.Replica;
import org.apache.hadoop.hdfs.server.datanode.ReplicaHandler;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInPipelineInterface;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.hdfs.server.datanode.ReplicaNotFoundException;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdfs.server.datanode.UnexpectedReplicaStateException;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaInputStreams;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaOutputStreams;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

@InterfaceAudience.Private
public class FsDatasetTier implements FsDatasetSpi<FsVolumeWrap> {
   static final Log LOG = LogFactory.getLog(FsDatasetTier.class);

   // Creating EnumMap in java with key as enum type StorageType
   private EnumMap<StorageType, FsDatasetSpi<? extends FsVolumeSpi>> tierMap;

   private List<FsVolumeWrap> volumes;

   // Initialization of tiers (MEMORY, SSD, DISK, REMOTE) from factory

   // Create an FsDatasetTier
   FsDatasetTier() throws IOException {
      this.tierMap = new EnumMap<StorageType, FsDatasetSpi<? extends FsVolumeSpi>>(
            StorageType.class);
      this.volumes = Collections
            .synchronizedList(new ArrayList<FsVolumeWrap>());
   }

   // Add a tier
   public void addTier(StorageType storage,
         FsDatasetSpi<? extends FsVolumeSpi> fsdataset) {
      this.tierMap.put(storage, fsdataset);

      // Add the tier volumes as well
      List<? extends FsVolumeSpi> vols = fsdataset.getVolumes();
      for (FsVolumeSpi vol : vols) {
         volumes.add(new FsVolumeWrap(vol));
      }
   }

   // ------------------------------------------------------------------------
   // Implementation of methods from FSDatasetMBean

   @Override
   public long getBlockPoolUsed(String bpid) throws IOException {

      long blockPoolUsed = 0;

      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {
         blockPoolUsed = blockPoolUsed + ds.getBlockPoolUsed(bpid);
      }

      return blockPoolUsed;
   }

   @Override
   public long getDfsUsed() throws IOException {
      long dfsUsed = 0;

      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {
         dfsUsed = dfsUsed + ds.getDfsUsed();
      }

      return dfsUsed;
   }

   @Override
   public long getCapacity() {
      long capacity = 0;

      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {
         capacity = capacity + ds.getCapacity();
      }

      return capacity;
   }

   @Override
   public long getCacheAvailable() throws IOException {
       long available = 0;

       for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {
          available = available + ds.getCacheAvailable();
       }

       return available;
   }
   
   @Override
   public long getRemaining() throws IOException {
      long remaining = 0;

      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {
         remaining = remaining + ds.getRemaining();
      }

      return remaining;
   }

   @Override
   public String getStorageInfo() {
      StringBuilder sb = new StringBuilder();
      sb.append("FSDatasetTier{");

      for (Entry<StorageType, FsDatasetSpi<? extends FsVolumeSpi>> entry : tierMap
            .entrySet()) {
         sb.append(entry.getKey());
         sb.append('=');
         sb.append(entry.getValue().getStorageInfo());
      }
      sb.append('}');

      return sb.toString();
   }

   @Override
   public int getNumFailedVolumes() {
      int numFailedVolumes = 0;

      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {
         numFailedVolumes = numFailedVolumes + ds.getNumFailedVolumes();
      }

      return numFailedVolumes;
   }

   @Override
   public String[] getFailedStorageLocations() {

      List<String> failedStorageLocations = new ArrayList<String>(
            volumes.size());
      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {

         String[] failStorLocs = ds.getFailedStorageLocations();
         for (String getFailStorLocs : failStorLocs)
            failedStorageLocations.add(getFailStorLocs);

      }
      return failedStorageLocations.toArray(new String[failedStorageLocations
            .size()]);

   }

   @Override
   public long getLastVolumeFailureDate() {
      long lastVolumeFailureDate = 0;

      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {
         long curr = ds.getLastVolumeFailureDate();
         if (curr > lastVolumeFailureDate)
            lastVolumeFailureDate = curr;
      }

      return lastVolumeFailureDate;
   }

   @Override
   public long getEstimatedCapacityLostTotal() {
      long estimatedCapacityLostTotal = 0;

      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {
         estimatedCapacityLostTotal = estimatedCapacityLostTotal
               + ds.getEstimatedCapacityLostTotal();
      }

      return estimatedCapacityLostTotal;
   }

   @Override
   public long getCacheUsed() throws IOException {
      long cacheUsed = 0;

      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {
         cacheUsed = cacheUsed + ds.getCacheUsed();
      }

      return cacheUsed;
   }

   @Override
   public long getCacheCapacity() {
      long cacheCapacity = 0;

      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {
         cacheCapacity = cacheCapacity + ds.getCacheCapacity();
      }

      return cacheCapacity;
   }

   @Override
   public long getNumBlocksCached() {
      long numBlocksCached = 0;

      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {
         numBlocksCached = numBlocksCached + ds.getNumBlocksCached();
      }

      return numBlocksCached;
   }

   @Override
   public long getNumBlocksFailedToCache() {
      long numBlocksFailedToCached = 0;

      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {
         numBlocksFailedToCached = numBlocksFailedToCached
               + ds.getNumBlocksFailedToCache();
      }

      return numBlocksFailedToCached;
   }

   @Override
   public long getNumBlocksFailedToUncache() {
      long numBlocksFailedToUncached = 0;

      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {
         numBlocksFailedToUncached = numBlocksFailedToUncached
               + ds.getNumBlocksFailedToUncache();
      }

      return numBlocksFailedToUncached;
   }

   // /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
   // Implementation of methods from FsDatasetSpi

   @Override
   public List<FsVolumeWrap> getVolumes() {
      return Collections.unmodifiableList(volumes);
   }

   /**
    * Add a new volume to the FsDataset.
    * <p/>
    *
    * If the FSDataset supports block scanning, this function registers the new
    * volume with the block scanner.
    *
    * @param location
    *           The storage location for the new volume.
    * @param nsInfos
    *           Namespace information for the new volume.
    */

   @Override
   public FsVolumeWrap addVolume(StorageLocation location,
         List<NamespaceInfo> nsInfos) throws IOException {

      FsVolumeWrap volume = null;

      FsDatasetSpi<? extends FsVolumeSpi> fsdataset = this.tierMap.get(location
            .getStorageType());

      if (fsdataset != null) {// check if the storage location exist
         volume = new FsVolumeWrap(fsdataset.addVolume(location, nsInfos));

         // Add the new volume in the list of volumes
         volumes.add(volume);
      } else
         // error message
         throw new IOException("Storage Location" + location.getStorageType()
               + " is not valid.");

      return volume;
   }

   /**
    * Removes a collection of volumes from FsDataset.
    *
    * If the FSDataset supports block scanning, this function removes the
    * volumes from the block scanner.
    *
    * @param volumes
    *           The paths of the volumes to be removed.
    * @param clearFailure
    *           set true to clear the failure information about the volumes.
    */
   @Override
   public List<FsVolumeWrap> removeVolumes(Set<File> volumes,
         boolean clearFailure) {

      List<FsVolumeWrap> removedVolumes = new ArrayList<FsVolumeWrap>();

      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {

         // Call the appropriate method based on storage type
         // Return the list of removed volumes
         List<? extends FsVolumeSpi> rVolumes = ds.removeVolumes(volumes,
               clearFailure);

         // remove the corresponding volume wraps
         synchronized (volumes) {
            Iterator<FsVolumeWrap> iter = this.volumes.iterator();
            while (iter.hasNext()) {
               FsVolumeWrap wrap = iter.next();
               for (FsVolumeSpi rVolume : rVolumes) {
                  if (wrap.getVolume().equals(rVolume)) {
                     removedVolumes.add(wrap);
                     iter.remove();
                     break;
                  }
               }
            }
         }

      }

      return removedVolumes;

   }

   /** @return a storage with the given storage ID */
   @Override
   public DatanodeStorage getStorage(String storageUuid) {

      DatanodeStorage storage = null;

      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {

         // Call the appropriate method based on storage type
         storage = ds.getStorage(storageUuid);

         if (storage != null)
            return storage;
      }

      return storage;

   }

   /** @return one or more storage reports for attached volumes. */
   @Override
   public StorageReport[] getStorageReports(String bpid) throws IOException {

      List<StorageReport> allReports = new ArrayList<StorageReport>(
            volumes.size());

      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {

         // Call the appropriate method based on storage type
         StorageReport[] storageReports = ds.getStorageReports(bpid);

         for (StorageReport report : storageReports)
            allReports.add(report);
      }

      return allReports.toArray(new StorageReport[allReports.size()]);
   }

   /** @return the volume that contains a replica of the block. */
   @Override
   public FsVolumeWrap getVolume(ExtendedBlock b) {

      FsDatasetSpi<? extends FsVolumeSpi> ds;
      for (StorageType type : StorageType.valuesOrdered()) {
         if (tierMap.containsKey(type)) {
            ds = tierMap.get(type);

            // Check which volume has the block
            if (ds.contains(b) == true) {
               FsVolumeSpi volume = ds.getVolume(b);
               if (volume != null)
                  return new FsVolumeWrap(volume);
            }
         }
      }

      return null;
   }

   /** @return a volume information map (name => info). */
   @Override
   public Map<String, Object> getVolumeInfoMap() {

      Map<String, Object> volumeInfoMap = new HashMap<String, Object>();

      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {

         Map<String, Object> localMap = ds.getVolumeInfoMap();
         if (localMap != null) {
            volumeInfoMap.putAll(localMap);
         }
      }

      return volumeInfoMap;
   }

   /**
    * Returns info about volume failures.
    *
    * @return info about volume failures, possibly null
    */
   @Override
   public VolumeFailureSummary getVolumeFailureSummary() {

      VolumeFailureSummary volumeFailureSummaries = null;
      List<VolumeFailureSummary> volumeFailureSummariesList = new ArrayList<VolumeFailureSummary>(
            tierMap.size());

      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {

         VolumeFailureSummary getVolFailSum = ds.getVolumeFailureSummary();
         if (getVolFailSum != null)
            volumeFailureSummariesList.add(getVolFailSum);

      }

      // Prepare the creation of the new VolumeFailureSummary from all datasets

      int size = 0;
      for (VolumeFailureSummary vFs : volumeFailureSummariesList) {
         size = size + vFs.getFailedStorageLocations().length;
      }
      String[] failedStorageLocations = new String[size];
      int index = 0;
      long lastVolumeFailureDate = 0;
      long estimatedCapacityLostTotal = 0;

      for (VolumeFailureSummary vFs : volumeFailureSummariesList) {
         String[] locs = vFs.getFailedStorageLocations();
         for (int i = 0; i < locs.length; i++) {
            failedStorageLocations[index] = locs[i];
            index++;
         }

         long lastFailDate = vFs.getLastVolumeFailureDate();
         if (lastFailDate > lastVolumeFailureDate)
            lastVolumeFailureDate = lastFailDate;

         estimatedCapacityLostTotal = estimatedCapacityLostTotal
               + vFs.getEstimatedCapacityLostTotal();
      }

      // Create the new VolumeFailureSummary from all datasets
      volumeFailureSummaries = new VolumeFailureSummary(failedStorageLocations,
            lastVolumeFailureDate, estimatedCapacityLostTotal);

      return volumeFailureSummaries;
   }

   /** @return a list of finalized blocks for the given block pool. */
   @Override
   public List<FinalizedReplica> getFinalizedBlocks(String bpid) {

      List<FinalizedReplica> finalizedReplicas = new ArrayList<FinalizedReplica>();

      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {

         List<FinalizedReplica> getFinBlocks = ds.getFinalizedBlocks(bpid);
         if (getFinBlocks != null)
            finalizedReplicas.addAll(getFinBlocks);

      }

      return finalizedReplicas;
   }

   /** @return a list of finalized blocks for the given block pool. */
   @Override
   public List<FinalizedReplica> getFinalizedBlocksOnPersistentStorage(
         String bpid) {

      List<FinalizedReplica> finalizedReplicas = new ArrayList<FinalizedReplica>();

      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {

         List<FinalizedReplica> getFinBlocks = ds
               .getFinalizedBlocksOnPersistentStorage(bpid);
         if (getFinBlocks != null)
            finalizedReplicas.addAll(getFinBlocks);

      }

      return finalizedReplicas;
   }

   /**
    * Check whether the in-memory block record matches the block on the disk,
    * and, in case that they are not matched, update the record or mark it as
    * corrupted.
    */
   @Override
   public void checkAndUpdate(String bpid, long blockId, File diskFile,
         File diskMetaFile, FsVolumeSpi vol, boolean finalizedORcached) throws IOException {

      if (vol instanceof FsVolumeWrap)
         vol = ((FsVolumeWrap) vol).getVolume();

      vol.getDataset().checkAndUpdate(bpid, blockId, diskFile, diskMetaFile,
            vol, finalizedORcached);
   }

   /**
    * @param b
    *           - the block
    * @return a stream if the meta-data of the block exists; otherwise, return
    *         null.
    * @throws IOException
    */
   @Override
   public LengthInputStream getMetaDataInputStream(ExtendedBlock b)
         throws IOException {

      FsDatasetSpi<? extends FsVolumeSpi> ds;
      for (StorageType type : StorageType.valuesOrdered()) {
         if (tierMap.containsKey(type)) {
            ds = tierMap.get(type);
   
            // Get the input stream from the first tier it contains the block
            if (ds.contains(b) == true) {
               return ds.getMetaDataInputStream(b);
            }
         }
      }

      return null;
   }

   /**
    * Returns the specified block's on-disk length (excluding metadata)
    * 
    * @return the specified block's on-disk length (excluding metadta)
    * @throws IOException
    *            on error
    */
   @Override
   public long getLength(ExtendedBlock b) throws IOException {

      FsDatasetSpi<? extends FsVolumeSpi> ds;
      for (StorageType type : StorageType.valuesOrdered()) {
         if (tierMap.containsKey(type)) {
            ds = tierMap.get(type);
   
            // Get the length from the first tier it contains the block
            if (ds.contains(b) == true) {
               return ds.getLength(b);
            }
         }
      }

      throw new IOException("BlockId " + b.getBlockId() + " is not valid.");
   }

   /**
    * Get reference to the replica meta info in the replicasMap. To be called
    * from methods that are synchronized on {@link FSDataset}
    * 
    * @return replica from the replicas map
    */
   @Deprecated
   public Replica getReplica(String bpid, long blockId) {
      return getReplica(bpid, blockId, null);
   }
   
   @Deprecated
   public Replica getReplica(String bpid, long blockId, StorageType skip) {
      Replica replica = null;

      FsDatasetSpi<? extends FsVolumeSpi> ds;
      for (StorageType type : StorageType.valuesOrdered()) {
         if (type != skip && tierMap.containsKey(type)) {
            ds = tierMap.get(type);
   
            replica = ds.getReplica(bpid, blockId);
            if (replica != null) {
               return replica;
            }
         }
      }

      return replica;

   }

   /**
    * @return replica meta information
    */
   @Override
   public String getReplicaString(String bpid, long blockId) {

      String replica = null;

      FsDatasetSpi<? extends FsVolumeSpi> ds;
      for (StorageType type : StorageType.valuesOrdered()) {
         if (tierMap.containsKey(type)) {
            ds = tierMap.get(type);
   
            // Call the appropriate method based on storage type
            replica = ds.getReplicaString(bpid, blockId);
   
            if (replica != null && !replica.equals("null"))
               return replica;
         }
      }

      return replica;
   }

   /**
    * @return the generation stamp stored with the block.
    */
   @Override
   public Block getStoredBlock(String bpid, long blkid) throws IOException {

      Block storedBlock = null;

      FsDatasetSpi<? extends FsVolumeSpi> ds;
      for (StorageType type : StorageType.valuesOrdered()) {
         if (tierMap.containsKey(type)) {
            ds = tierMap.get(type);
   
            // Call the appropriate method based on storage type
            storedBlock = ds.getStoredBlock(bpid, blkid);
   
            if (storedBlock != null)
               return storedBlock;
         }
      }

      return storedBlock;
   }

   /**
    * Returns an input stream at specified offset of the specified block
    * 
    * @param b
    *           block
    * @param seekOffset
    *           offset with in the block to seek to
    * @return an input stream to read the contents of the specified block,
    *         starting at the offset
    * @throws IOException
    */
   @Override
   public InputStream getBlockInputStream(ExtendedBlock b, long seekOffset)
         throws IOException {

      FsDatasetSpi<? extends FsVolumeSpi> ds;
      for (StorageType type : StorageType.valuesOrdered()) {
         if (tierMap.containsKey(type)) {
            ds = tierMap.get(type);
   
            // Get the input stream from the first tier it contains the block
            if (ds.contains(b) == true) {
               return ds.getBlockInputStream(b, seekOffset);
            }
         }
      }

      throw new IOException("BlockId " + b + " is not valid.");
   }

   /**
    * Returns an input stream at specified offset of the specified block The
    * block is still in the tmp directory and is not finalized
    * 
    * @return an input stream to read the contents of the specified block,
    *         starting at the offset
    * @throws IOException
    */
   @Override
   public ReplicaInputStreams getTmpInputStreams(ExtendedBlock b, long blkoff,
         long ckoff) throws IOException {

      ReplicaInputStreams tmpInputStreams = null;

      FsDatasetSpi<? extends FsVolumeSpi> ds;
      for (StorageType type : StorageType.valuesOrdered()) {
         if (tierMap.containsKey(type)) {
            ds = tierMap.get(type);
   
            // Get the input stream from the first tier it contains the block
            if (ds.contains(b) == true) {
               tmpInputStreams = ds.getTmpInputStreams(b, blkoff, ckoff);
               return tmpInputStreams;
            }
         }
      }

      throw new IOException("BlockId " + b + " is not valid.");
   }

   /**
    * Creates a temporary replica and returns the meta information of the
    * replica
    * 
    * @param b
    *           block
    * @return the meta info of the replica which is being written to
    * @throws IOException
    *            if an error occurs
    */
   @Override
   public ReplicaHandler createTemporary(String volumeId, StorageType storageType, ExtendedBlock b)
           throws IOException {

       FsDatasetSpi<? extends FsVolumeSpi> ds = tierMap.get(storageType);
       if (ds == null)
           throw new IOException("Unable to find dataset with type " + storageType);
       
       return ds.createTemporary(volumeId, storageType, b);
   }


   /**
    * Creates a RBW replica and returns the meta info of the replica
    * 
    * @param b
    *           block
    * @return the meta info of the replica which is being written to
    * @throws IOException
    *            if an error occurs
    */
   @Override
   public ReplicaHandler createRbw(StorageType storageType, ExtendedBlock b,
         boolean allowLazyPersist) throws IOException {

      FsDatasetSpi<? extends FsVolumeSpi> ds = tierMap.get(storageType);
      if (ds == null)
          throw new IOException("Unable to find dataset with type " + storageType);
      
      return ds.createRbw(storageType, b, allowLazyPersist);
   }

   /**
    * Recovers a RBW replica and returns the meta info of the replica
    * 
    * @param b
    *           block
    * @param newGS
    *           the new generation stamp for the replica
    * @param minBytesRcvd
    *           the minimum number of bytes that the replica could have
    * @param maxBytesRcvd
    *           the maximum number of bytes that the replica could have
    * @return the meta info of the replica which is being written to
    * @throws IOException
    *            if an error occurs
    */
   @Override
   public ReplicaHandler recoverRbw(ExtendedBlock b, long newGS,
         long minBytesRcvd, long maxBytesRcvd) throws IOException {

      FsDatasetSpi<? extends FsVolumeSpi> ds;
      for (StorageType type : StorageType.valuesOrdered()) {
         if (tierMap.containsKey(type)) {
            ds = tierMap.get(type);

            // Recover from the tier that has the RBW block
            if (ds.isValidRbw(b)) {
               return ds.recoverRbw(b, newGS, minBytesRcvd, maxBytesRcvd);
            }
         }
      }

      throw new ReplicaNotFoundException(
            ReplicaNotFoundException.NON_EXISTENT_REPLICA + b);
   }

   /**
    * Covert a temporary replica to a RBW.
    * 
    * @param temporary
    *           the temporary replica being converted
    * @return the result RBW
    */
   @Override
   public ReplicaInPipelineInterface convertTemporaryToRbw(
         ExtendedBlock temporary) throws IOException {

      FsDatasetSpi<? extends FsVolumeSpi> ds;
      for (StorageType type : StorageType.valuesOrdered()) {
         if (tierMap.containsKey(type)) {
            ds = tierMap.get(type);
   
            try {
               ds.checkBlock(temporary, 0, ReplicaState.TEMPORARY);
               // Call the appropriate method based on storage type
               return ds.convertTemporaryToRbw(temporary);
            } catch (IOException e) {
               // ignore it
            }
         }
      }

      throw new ReplicaNotFoundException(
            ReplicaNotFoundException.NON_EXISTENT_REPLICA + temporary);
   }

   /**
    * Append to a finalized replica and returns the meta info of the replica
    * 
    * @param b
    *           block
    * @param newGS
    *           the new generation stamp for the replica
    * @param expectedBlockLen
    *           the number of bytes the replica is expected to have
    * @return the meata info of the replica which is being written to
    * @throws IOException
    */
   @Override
   public ReplicaHandler append(StorageType type, ExtendedBlock b, long newGS,
         long expectedBlockLen) throws IOException {

       FsDatasetSpi<? extends FsVolumeSpi> ds = tierMap.get(type);
       if (ds == null)
           throw new IOException("Unable to find dataset with type " + type);

       return ds.append(type, b, newGS, expectedBlockLen);
   }

   /**
    * Recover a failed append to a finalized replica and returns the meta info
    * of the replica
    * 
    * @param b
    *           block
    * @param newGS
    *           the new generation stamp for the replica
    * @param expectedBlockLen
    *           the number of bytes the replica is expected to have
    * @return the meta info of the replica which is being written to
    * @throws IOException
    */
   @Override
   public ReplicaHandler recoverAppend(ExtendedBlock b, long newGS,
         long expectedBlockLen) throws IOException {

      FsDatasetSpi<? extends FsVolumeSpi> ds;
      for (StorageType type : StorageType.valuesOrdered()) {
         if (tierMap.containsKey(type)) {
            ds = tierMap.get(type);

            // Check which tier has the block
            if (ds.isValidBlock(b) || ds.isValidRbw(b)) {
               return ds.recoverAppend(b, newGS, expectedBlockLen);
            }
         }
      }

      throw new ReplicaNotFoundException(
            ReplicaNotFoundException.NON_EXISTENT_REPLICA + b);
   }

   /**
    * Recover a failed pipeline close It bumps the replica's generation stamp
    * and finalize it if RBW replica
    * 
    * @param b
    *           block
    * @param newGS
    *           the new generation stamp for the replica
    * @param expectedBlockLen
    *           the number of bytes the replica is expected to have
    * @return the storage uuid of the replica.
    * @throws IOException
    */
   @Override
   public String recoverClose(ExtendedBlock b, long newGS, long expectedBlockLen)
         throws IOException {

      FsDatasetSpi<? extends FsVolumeSpi> ds;
      for (StorageType type : StorageType.valuesOrdered()) {
         if (tierMap.containsKey(type)) {
            ds = tierMap.get(type);

            // Check which tier has the block
            if (ds.isValidBlock(b) || ds.isValidRbw(b)) {
               return ds.recoverClose(b, newGS, expectedBlockLen);
            }
         }
      }

      throw new ReplicaNotFoundException(
            ReplicaNotFoundException.NON_EXISTENT_REPLICA + b);
   }

   /**
    * Finalizes the block previously opened for writing using writeToBlock. The
    * block size is what is in the parameter b and it must match the amount of
    * data written
    * 
    * @throws IOException
    * @throws ReplicaNotFoundException
    *            if the replica can not be found when the block is been
    *            finalized. For instance, the block resides on an HDFS volume
    *            that has been removed.
    */
   @Override
   public void finalizeBlock(ExtendedBlock b) throws IOException {

      boolean found = false;

      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {

         // Check which tier has the block
         if (ds.contains(b) && !ds.isValidBlock(b)) {
            ds.finalizeBlock(b);
            found = true;
         }

      }

      if (!found)
         throw new ReplicaNotFoundException(
               ReplicaNotFoundException.NON_EXISTENT_REPLICA + b);
   }

   /**
    * Unfinalizes the block previously opened for writing using writeToBlock.
    * The temporary file associated with this block is deleted.
    * 
    * @throws IOException
    */
   @Override
   public void unfinalizeBlock(ExtendedBlock b) throws IOException {

      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {

         // Check which volume has the block
         if (ds.contains(b)) {
            ds.unfinalizeBlock(b);
         }
      }

   }

   /**
    * Returns one block report per volume.
    * 
    * @param bpid
    *           Block Pool Id
    * @return - a map of DatanodeStorage to block report for the volume.
    */
   @Override
   public Map<DatanodeStorage, BlockListAsLongs> getBlockReports(String bpid) {

      Map<DatanodeStorage, BlockListAsLongs> blockReports = new HashMap<DatanodeStorage, BlockListAsLongs>();

      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {

         // Call the appropriate method based on storage type
         Map<DatanodeStorage, BlockListAsLongs> blockReps = ds
               .getBlockReports(bpid);
         if (blockReps != null)
            blockReports.putAll(blockReps);

      }

      return blockReports;
   }

   /**
    * Returns the cache report - the full list of cached block IDs of a block
    * pool.
    * 
    * @param bpid
    *           Block Pool Id
    * @return the cache report - the full list of cached block IDs.
    */
   @Override
   public Map<DatanodeStorage, List<Long>> getCacheReport(String bpid) {

       Map<DatanodeStorage, List<Long>> cacheReports = new HashMap<DatanodeStorage, List<Long>>();

       for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {

           // Call the appropriate method based on storage type
           Map<DatanodeStorage, List<Long>> cacheReps = ds.getCacheReport(bpid);
           if (cacheReps != null)
               cacheReports.putAll(cacheReps);
       }

       return cacheReports;
   }

   /** Does any dataset contain the block? */
   @Override
   public boolean contains(ExtendedBlock block) {

      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {
         if (ds.contains(block))
            return true;
      }

      return false;
   }

   /**
    * Check if a block is valid.
    *
    * @param b
    *           The block to check.
    * @param minLength
    *           The minimum length that the block must have. May be 0.
    * @param state
    *           If this is null, it is ignored. If it is non-null, we will check
    *           that the replica has this state.
    *
    * @throws ReplicaNotFoundException
    *            If the replica is not found
    *
    * @throws UnexpectedReplicaStateException
    *            If the replica is not in the expected state.
    * @throws FileNotFoundException
    *            If the block file is not found or there was an error locating
    *            it.
    * @throws EOFException
    *            If the replica length is too short.
    * 
    * @throws IOException
    *            May be thrown from the methods called.
    */
   @Override
   public void checkBlock(ExtendedBlock b, long minLength, ReplicaState state)
         throws ReplicaNotFoundException, UnexpectedReplicaStateException,
         FileNotFoundException, EOFException, IOException {

      Exception currException = null;
      int currPriority = 0;

      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {

         try {
            // Check which tier has the block
            if (ds.contains(b)) {
               ds.checkBlock(b, minLength, state);
               return; // check succeeded
            }
         } catch (ReplicaNotFoundException e) {
            if (currPriority <= 1) {
               currException = e;
               currPriority = 1;
               continue;
            }
         } catch (UnexpectedReplicaStateException e) {
            if (currPriority <= 2) {
               currPriority = 2;
               currException = e;
               continue;
            }

         } catch (FileNotFoundException e) {
            if (currPriority <= 3) {
               currPriority = 3;
               currException = e;
               continue;
            }
         } catch (EOFException e) {
            if (currPriority <= 4) {
               currPriority = 4;
               currException = e;
               continue;
            }
         } catch (IOException e) {
            if (currPriority <= 5) {
               currPriority = 5;
               currException = e;
               continue;
            }
         }
      }

      if (currException == null)
         throw new ReplicaNotFoundException(b);
      else {
         switch (currPriority) {
         case 1:
            throw (ReplicaNotFoundException) currException;
         case 2:
            throw (UnexpectedReplicaStateException) currException;
         case 3:
            throw (FileNotFoundException) currException;
         case 4:
            throw (EOFException) currException;
         case 5:
            throw (IOException) currException;
         default:
            throw new IOException("Unexpected exception", currException);
         }
      }

   }

   /**
    * Is the block valid?
    * 
    * @return - true if the specified block is valid
    */
   @Override
   public boolean isValidBlock(ExtendedBlock b) {

      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {

         if (ds.contains(b) && ds.isValidBlock(b))
            return true;
      }

      return false;
   }

   /**
    * Is the block a valid RBW?
    * 
    * @return - true if the specified block is a valid RBW
    */
   @Override
   public boolean isValidRbw(ExtendedBlock b) {

      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {

         if (ds.contains(b) && ds.isValidRbw(b))
            return true;
      }

      return false;
   }

   /**
    * Invalidates the specified blocks
    * 
    * @param bpid
    *           Block pool Id
    * @param invalidBlks
    *           - the blocks to be invalidated
    * @throws IOException
    */
   @Override
   public void invalidate(String bpid, Block[] invalidBlks, StorageType[] storageTypes) throws IOException {

       Block[] invBlk = new Block[1];
       StorageType[] storage = new StorageType[1];
       
       for (int s=0; s<storageTypes.length; s++) {

           FsDatasetSpi<? extends FsVolumeSpi> ds = tierMap.get(storageTypes[s]);
           if (ds == null)
               throw new IOException("Unable to find dataset with type " + storageTypes[s]);
           
           if (ds.contains(bpid, invalidBlks[s].getBlockId()) == true) {

               invBlk[0] = invalidBlks[s];
               storage[0] = storageTypes[s];
               
               // Call the appropriate method based on storage type
               ds.invalidate(bpid, invBlk, storage);

           }
           else{
              LOG.warn("BlockId " + invalidBlks[s].getBlockId() +
                       " is not in storage type " + storageTypes[s] +
                       ". Is it in datanode? " + 
                       contains(bpid, invalidBlks[s].getBlockId()));
           }
       }
   }

   /**
    * Caches the specified blocks
    * 
    * @param bpid
    *           Block pool id
    * @param blockIds
    *           - block ids to cache
    */
   @Override
   public void cache(String bpid, long[] blockIds) {

      long[] blkIds = new long[1];

      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {

         // Find first where are the blocks (the volume belong)
         // Make for loop for long[] blockIds
         for (int i = 0; i < blockIds.length; i++) {
            if (ds.contains(bpid, blockIds[i]) == true) {

               blkIds[0] = blockIds[i];

               // Call the appropriate method based on storage type
               ds.cache(bpid, blkIds);

            }

         }
      }

   }

   /**
    * Uncaches the specified blocks
    * 
    * @param bpid
    *           Block pool id
    * @param blockIds
    *           - blocks ids to uncache
 * @throws IOException 
    */
   @Override
   public void uncache(String bpid, long[] blockIds, StorageType type) throws IOException {
       // Call the appropriate method based on storage type
       FsDatasetSpi<? extends FsVolumeSpi> ds = tierMap.get(type);
       if (ds == null)
           throw new IOException("Unable to find dataset with type " + type);

       ds.uncache(bpid, blockIds, type);
   }

   /**
    * Determine if the specified block is cached.
    * 
    * @param bpid
    *           Block pool id
    * @param blockIds
    *           - block id
    * @return true if the block is cached
    */
   @Override
   public boolean isCached(String bpid, long blockId) {

      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {

         if (ds.isCached(bpid, blockId))
            return true;
      }

      return false;
   }

   /**
    * Check if all the data directories are healthy
    * 
    * @return A set of unhealthy data directories.
    */
   @Override
   public Set<File> checkDataDir() {

      Set<File> unhealthyDir = new HashSet<File>();

      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {

         Set<File> checkDD = ds.checkDataDir();
         // Call the appropriate method based on storage type
         if (checkDD != null)
            unhealthyDir.addAll(checkDD);
      }

      return unhealthyDir;
   }

   /**
    * Shutdown the FSDataset
    */
   @Override
   public void shutdown() {

      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {
         // Call the appropriate method based on storage type
         ds.shutdown();
      }

   }

   /**
    * Sets the file pointer of the checksum stream so that the last checksum
    * will be overwritten
    * 
    * @param b
    *           block
    * @param outs
    *           The streams for the data file and checksum file
    * @param checksumSize
    *           number of bytes each checksum has
    * @throws IOException
    */
   @Override
   public void adjustCrcChannelPosition(ExtendedBlock b,
         ReplicaOutputStreams outs, int checksumSize) throws IOException {

       FsDatasetSpi<? extends FsVolumeSpi> ds = tierMap.get(outs.getStorageType());
       if (ds == null)
           throw new IOException("Unable to find dataset with type " + outs.getStorageType());

       ds.adjustCrcChannelPosition(b, outs,
               checksumSize);
   }

   /**
    * Checks how many valid storage volumes there are in the DataNode.
    * 
    * @return true if more than the minimum number of valid volumes are left in
    *         the FSDataSet.
    */
   @Override
   public boolean hasEnoughResource() {

      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {

         // Check if any volume hasEnoughResource
         if (ds.hasEnoughResource()) {
            return true;
         }
      }

      return false;
   }

   /**
    * Get visible length of the specified replica.
    */
   @Override
   public long getReplicaVisibleLength(ExtendedBlock block) throws IOException {

      FsDatasetSpi<? extends FsVolumeSpi> ds;
      for (StorageType type : StorageType.valuesOrdered()) {
         if (tierMap.containsKey(type)) {
            ds = tierMap.get(type);

            // Check which tier has the block
            if (ds.contains(block)) {
               return ds.getReplicaVisibleLength(block);
            }
         }
      }

      throw new ReplicaNotFoundException(
            ReplicaNotFoundException.NON_EXISTENT_REPLICA + block);
   }

   /**
    * Initialize a replica recovery.
    * 
    * @return actual state of the replica on this data-node or null if data-node
    *         does not have the replica.
    */
   @Override
   public ReplicaRecoveryInfo initReplicaRecovery(RecoveringBlock rBlock)
         throws IOException {
      FsDatasetSpi<? extends FsVolumeSpi> ds;
      for (StorageType type : StorageType.valuesOrdered()) {
         if (tierMap.containsKey(type)) {
            ds = tierMap.get(type);
   
            if (ds.contains(rBlock.getBlock()) == true) {
               return ds.initReplicaRecovery(rBlock);
            }
         }
      }

      return null;
   }

   /**
    * Update replica's generation stamp and length and finalize it.
    * 
    * @return the ID of storage that stores the block
    */
   @Override
   public String updateReplicaUnderRecovery(ExtendedBlock oldBlock,
         long recoveryId, long newBlockId, long newLength) throws IOException {

      FsDatasetSpi<? extends FsVolumeSpi> ds;
      for (StorageType type : StorageType.valuesOrdered()) {
         if (tierMap.containsKey(type)) {
            ds = tierMap.get(type);

            if (ds.contains(oldBlock)) {
               try {
                  checkBlock(oldBlock, 0, ReplicaState.RUR);
                  return ds.updateReplicaUnderRecovery(oldBlock, recoveryId,
                        newBlockId, newLength);
               } catch (IOException e) {
                  // ignore
               }
            }
         }
      }

      throw new ReplicaNotFoundException(
            ReplicaNotFoundException.NON_EXISTENT_REPLICA + oldBlock);
   }

   /**
    * add new block pool ID
    * 
    * @param bpid
    *           Block pool Id
    * @param conf
    *           Configuration
    */
   @Override
   public void addBlockPool(String bpid, Configuration conf) throws IOException {
      // Add block pool to all tiers
      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {
         ds.addBlockPool(bpid, conf);
      }

   }

   /**
    * Shutdown and remove the block pool from underlying storage.
    * 
    * @param bpid
    *           Block pool Id to be removed
    */
   @Override
   public void shutdownBlockPool(String bpid) {
      // Shutdown from all tiers
      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {
         ds.shutdownBlockPool(bpid);
      }

   }

   /**
    * Deletes the block pool directories. If force is false, directories are
    * deleted only if no block files exist for the block pool. If force is true
    * entire directory for the blockpool is deleted along with its contents.
    * 
    * @param bpid
    *           BlockPool Id to be deleted.
    * @param force
    *           If force is false, directories are deleted only if no block
    *           files exist for the block pool, otherwise entire directory for
    *           the blockpool is deleted along with its contents.
    * @throws IOException
    */
   @Override
   public void deleteBlockPool(String bpid, boolean force) throws IOException {
      // Delete from all tiers
      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {
         ds.deleteBlockPool(bpid, force);
      }

   }

   /**
    * Get {@link BlockLocalPathInfo} for the given block.
    */
   @Override
   public BlockLocalPathInfo getBlockLocalPathInfo(ExtendedBlock b)
         throws IOException {

      FsDatasetSpi<? extends FsVolumeSpi> ds;
      for (StorageType type : StorageType.valuesOrdered()) {
         if (tierMap.containsKey(type)) {
            ds = tierMap.get(type);
            
            // Check which tier has the block
            if (ds.contains(b)) {
               BlockLocalPathInfo info = ds.getBlockLocalPathInfo(b);
               if (info != null)
                  return info;
            }
         }
      }

      throw new ReplicaNotFoundException(b);
   }

   /**
    * Get a {@link HdfsBlocksMetadata} corresponding to the list of blocks in
    * <code>blocks</code>.
    * 
    * @param bpid
    *           pool to query
    * @param blockIds
    *           List of block ids for which to return metadata
    * @return metadata Metadata for the list of blocks
    * @throws IOException
    */
   @Override
   public HdfsBlocksMetadata getHdfsBlocksMetadata(String bpid, long[] blockIds)
         throws IOException {

      HdfsBlocksMetadata hdfsBlocksMetadata = null;
      long[] blkIds = new long[1];
      List<HdfsBlocksMetadata> hdfsBlocksMetadataList = new ArrayList<HdfsBlocksMetadata>();

      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {

         // Find first where are the blocks (the volume belong)
         // Make for loop for long[] blockIds
         for (int i = 0; i < blockIds.length; i++) {
            if (ds.contains(bpid, blockIds[i]) == true) {

               blkIds[0] = blockIds[i];
               // Call the appropriate method based on storage type
               HdfsBlocksMetadata hdfsBlocksMetaData = ds
                     .getHdfsBlocksMetadata(bpid, blkIds);
               if (hdfsBlocksMetaData != null)
                  hdfsBlocksMetadataList.add(hdfsBlocksMetaData);

            }

         }
      }

      // Prepare the creation of the new HdfsBlocksMetadata from all datasets
      List<byte[]> volumeIds = new ArrayList<byte[]>();
      List<Integer> volumeIndexes = new ArrayList<Integer>();
      int currentSize = 0;

      for (HdfsBlocksMetadata hBm : hdfsBlocksMetadataList) {

         List<byte[]> getVolIds = hBm.getVolumeIds();

         volumeIds.addAll(getVolIds);

         List<Integer> getVolInd = hBm.getVolumeIndexes();

         for (int i = 0; i < getVolInd.size(); i++) {
            if (getVolInd.get(i) != Integer.MAX_VALUE)
               getVolInd.set(i, getVolInd.get(i) + currentSize);
            else
               getVolInd.set(i, getVolInd.get(i));
         }

         volumeIndexes.addAll(getVolInd);

         currentSize = currentSize + getVolIds.size();
      }

      // Create the new HdfsBlocksMetadata from all datasets
      hdfsBlocksMetadata = new HdfsBlocksMetadata(bpid, blockIds, volumeIds,
            volumeIndexes);

      return hdfsBlocksMetadata;
   }

   /**
    * Enable 'trash' for the given dataset. When trash is enabled, files are
    * moved to a separate trash directory instead of being deleted immediately.
    * This can be useful for example during rolling upgrades.
    */
   @Override
   public void enableTrash(String bpid) {
      // Enable trash for all tiers
      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {
         ds.enableTrash(bpid);
      }

   }

   /**
    * Restore trash
    */
   @Override
   public void restoreTrash(String bpid) {
      // Restore trash for all tiers
      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {
         ds.restoreTrash(bpid);
      }

   }

   /**
    * @return true when trash is enabled
    */
   @Override
   public boolean trashEnabled(String bpid) {

      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {
         if (ds.trashEnabled(bpid))
            return true;
      }

      return false;
   }

   /**
    * Create a marker file indicating that a rolling upgrade is in progress.
    */
   @Override
   public void setRollingUpgradeMarker(String bpid) throws IOException {
      // Set marker for all tiers
      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {
         ds.setRollingUpgradeMarker(bpid);
      }

   }

   /**
    * Delete the rolling upgrade marker file if it exists.
    * 
    * @param bpid
    */
   @Override
   public void clearRollingUpgradeMarker(String bpid) throws IOException {
      // Delete marker from all tiers
      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {
         ds.clearRollingUpgradeMarker(bpid);
      }

   }

   /**
    * submit a sync_file_range request to AsyncDiskService
    */
   @Override
   public void submitBackgroundSyncFileRangeRequest(ExtendedBlock block,
         FileDescriptor fd, long offset, long nbytes, int flags) {

      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {

         // Check which volume has the block
         if (ds.contains(block) == true) {
            ds.submitBackgroundSyncFileRangeRequest(block, fd, offset, nbytes,
                  flags);
         }

      }

   }

   /**
    * Callback from RamDiskAsyncLazyPersistService upon async lazy persist task
    * end
    */
   @SuppressWarnings("unchecked")
   @Override
   public void onCompleteLazyPersist(String bpId, long blockId,
         long creationTime, File[] savedFiles, FsVolumeWrap targetVolume) {

      targetVolume
            .getVolume()
            .getDataset()
            .onCompleteLazyPersist(bpId, blockId, creationTime, savedFiles,
                  targetVolume.getVolume());

   }

   /**
    * Callback from RamDiskAsyncLazyPersistService upon async lazy persist task
    * fail
    */
   @Override
   public void onFailLazyPersist(String bpId, long blockId) {

      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {

         // Check which volume has the block
         if (ds.contains(bpId, blockId) == true) {
            // Call the appropriate method based on storage type
            ds.onFailLazyPersist(bpId, blockId);

         }
      }

   }

   /**
    * Move block from one storage to another storage
    */
   @Override
   public ReplicaInfo moveBlockAcrossStorage(ExtendedBlock block,
         StorageType targetStorageType) throws IOException {
      ReplicaInfo move = null;
      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {

         // Check which volume has the block
         if (ds.contains(block) == true) {
            // Call the appropriate method based on storage type
            move = ds.moveBlockAcrossStorage(block, targetStorageType);
            return move;
         }

      }

      return move;
   }

   /**
    * Set a block to be pinned on this datanode so that it cannot be moved by
    * Balancer/Mover.
    *
    * It is a no-op when dfs.datanode.block-pinning.enabled is set to false.
    */
   @Override
   public void setPinning(ExtendedBlock block) throws IOException {
      boolean found = false;

      // Pin on all tiers that contain the block
      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {
         if (ds.contains(block)) {
            ds.setPinning(block);
            found = true;
         }
      }

      if (!found)
         throw new IOException("BlockId " + block.getBlockId()
               + " is not valid.");
   }

   /**
    * Check whether the block was pinned
    */
   @Override
   public boolean getPinning(ExtendedBlock block) throws IOException {

      boolean found = false;

      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {
         // Check which tier has the block
         if (ds.contains(block)) {
            found = true;

            if (ds.getPinning(block))
               return true;
         }

      }

      if (found)
         return false; // Block was found but pinning is false
      else
         throw new IOException("BlockId " + block.getBlockId()
               + " is not valid.");
   }

   /**
    * Confirm whether the block is deleting
    */
   @Override
   public boolean isDeletingBlock(String bpid, long blockId) {

      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {
         if (ds.isDeletingBlock(bpid, blockId))
            return true;
      }

      return false;
   }

   @Override
   public boolean contains(String bpid, long blockId) {

      for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {
         if (ds.contains(bpid, blockId))
            return true;
      }

      return false;
   }

   @Override
   public long getWriteThroughput() {
       long writeAverageThroughput = 0;

       for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {
           writeAverageThroughput = writeAverageThroughput + ds.getWriteThroughput();
       }

       return writeAverageThroughput / tierMap.size();
   }

   @Override
   public long getReadThroughput() {
       long readAverageThroughput = 0;

       for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {
           readAverageThroughput = readAverageThroughput + ds.getReadThroughput();
       }

       return readAverageThroughput / tierMap.size();
   }

   @Override
   public void updatePerformanceMetrics() throws IOException {
       for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {
           ds.updatePerformanceMetrics();
       }
   }

   @Override
   public List<CachedReplica> getCachedBlocks(String bpId) {

       List<CachedReplica> cachedReplica = new ArrayList<CachedReplica>();

       for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {

           List<CachedReplica> getCachedBlks = ds.getCachedBlocks(bpId);
           if (getCachedBlks != null)
               cachedReplica.addAll(getCachedBlks);

       }

       return cachedReplica;
   }

   @Override
   public ReplicaHandler createCachingBlock(String volumeId, StorageType storageType, ExtendedBlock b) throws IOException {
       FsDatasetSpi<? extends FsVolumeSpi> ds = tierMap.get(storageType);
       if (ds == null)
           throw new IOException("Unable to find dataset with type " + storageType);

       return ds.createCachingBlock(volumeId, storageType, b);
   }

   @Override
   public void finalizeCachedBlock(ExtendedBlock b) throws IOException {
       boolean found = false;

       for (FsDatasetSpi<? extends FsVolumeSpi> ds : tierMap.values()) {

           // Check which tier has the block
           if (ds.contains(b) && !ds.isCached(b.getBlockPoolId(), b.getBlockId())) {
               ds.finalizeCachedBlock(b);
               found = true;
           }
       }

       if (!found)
           throw new ReplicaNotFoundException(
                   ReplicaNotFoundException.NON_EXISTENT_REPLICA + b);
   }

}
