package org.apache.hadoop.hdfs.server.datanode.fsdataset.tier;

import java.io.File;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;

public class FsVolumeWrap implements FsVolumeSpi {

   // member for appropriate dataset
   private FsVolumeSpi volume;

   // Constructor
   public FsVolumeWrap(FsVolumeSpi dataS) {

      this.volume = dataS;
   }

   @Override
   public FsVolumeReference obtainReference() throws ClosedChannelException {

      return volume.obtainReference();
   }

   @Override
   public String getStorageID() {

      return volume.getStorageID();
   }

   @Override
   public String[] getBlockPoolList() {

      return volume.getBlockPoolList();
   }

   @Override
   public long getCapacity() {
      return volume.getCapacity();
   }

   @Override
   public long getAvailable() throws IOException {

      return volume.getAvailable();
   }

   @Override
   public String getBasePath() {

      return volume.getBasePath();
   }

   @Override
   public String getPath(String bpid) throws IOException {

      return volume.getPath(bpid);
   }

   @Override
   public File getFinalizedDir(String bpid) throws IOException {

      return volume.getFinalizedDir(bpid);
   }

   @Override
   public StorageType getStorageType() {

      return volume.getStorageType();
   }

   @Override
   public void reserveSpaceForRbw(long bytesToReserve) {
      volume.reserveSpaceForRbw(bytesToReserve);

   }

   @Override
   public void releaseReservedSpace(long bytesToRelease) {
      volume.releaseReservedSpace(bytesToRelease);

   }

   @Override
   public boolean isTransientStorage() {

      return volume.isTransientStorage();
   }

   @Override
   public BlockIterator newBlockIterator(String bpid, String name) {

      return volume.newBlockIterator(bpid, name);
   }

   @Override
   public BlockIterator loadBlockIterator(String bpid, String name)
         throws IOException {

      return volume.loadBlockIterator(bpid, name);
   }

   @SuppressWarnings("rawtypes")
   @Override
   public FsDatasetSpi getDataset() {

      return volume.getDataset();
   }

   // Added by elena
   public FsVolumeSpi getVolume() {

      return volume;
   }

   @Override
   public long getWriteThroughput() {

       return volume.getWriteThroughput();
   }

   @Override
   public long getReadThroughput() {

       return volume.getReadThroughput();
   }

   @Override
   public void setWriteThroughput(long writeThroughput){
       volume.setWriteThroughput(writeThroughput); 
   }

   @Override
   public void setReadThroughput(long readThroughput){
       volume.setReadThroughput(readThroughput); 
   }

   @Override
   public File getCachedDir(String bpid) throws IOException {
       return volume.getCachedDir(bpid);
   }

   @Override
   public long getCacheAvailable() throws IOException {
       return volume.getCacheAvailable();
   }

   @Override
   public int getXceiverCount() {
      return volume.getXceiverCount();
   }
   
   @Override
   public int getReaderCount(){
      return volume.getReaderCount();
   }

   @Override
   public void incrReaderCount() {
      volume.incrReaderCount();
   }
   
   @Override
   public void decrReaderCount() {
      volume.decrReaderCount();
   }

   @Override
   public int getWriterCount() {
      return volume.getWriterCount();
   }

   @Override
   public void incrWriterCount() {
      volume.incrWriterCount();
   }
   
   @Override
   public void decrWriterCount() {
      volume.decrWriterCount();
   }
   

}
