/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapreduce.split;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.SplitLocationInfo;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * This class groups the fundamental classes associated with
 * reading/writing splits. The split information is divided into
 * two parts based on the consumer of the information. The two
 * parts are the split meta information, and the raw split 
 * information. The first part is consumed by the JobTracker to
 * create the tasks' locality data structures. The second part is
 * used by the maps at runtime to know what to do!
 * These pieces of information are written to two separate files.
 * The metainformation file is slurped by the JobTracker during 
 * job initialization. A map task gets the meta information during
 * the launch and it reads the raw split bytes directly from the 
 * file.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class JobSplit {
   static final Log LOG = LogFactory.getLog(JobSplit.class);
   
  static final int META_SPLIT_VERSION = 1;
  static final int SINGLE_FILE = 1;
  static final byte[] META_SPLIT_FILE_HEADER;
  static {
    try {
      META_SPLIT_FILE_HEADER = "META-SPL".getBytes("UTF-8");
    } catch (UnsupportedEncodingException u) {
      throw new RuntimeException(u);
    }
  } 
  public static final TaskSplitMetaInfo EMPTY_TASK_SPLIT = 
    new TaskSplitMetaInfo();
  
  /**
   * This represents the meta information about the task split.
   * The main fields are 
   *     - start offset in actual split
   *     - data length that will be processed in this split
   *     - hosts on which this split is local
   */
  public static class SplitMetaInfo implements Writable {
    private long startOffset;
    private long inputDataStart; // only applies to FileSplit
    private long inputDataLength;
    private Path filePath;
    private String[] locations;
    private StorageType[] storageTypes;
    private boolean[] isCached;

    public SplitMetaInfo() {}

    public SplitMetaInfo(Path filePath, String[] locations, StorageType[] storageTypes,
          SplitLocationInfo[] splitInfo, long startOffset, long inputDataStart, long inputDataLength) {
      this.filePath = filePath;
      this.locations = locations;
      this.storageTypes = storageTypes;
      this.startOffset = startOffset;
      this.inputDataStart = inputDataStart;
      this.inputDataLength = inputDataLength;
      setIsCached(splitInfo);
      
      if (LOG.isDebugEnabled())
         LOG.debug("SplitMetaInfo: " + toOneString());
    }
    
    public SplitMetaInfo(InputSplit split, long startOffset) throws IOException {
      try {
        this.filePath = split.getPath();
        this.locations = split.getLocations();
        this.storageTypes = split.getStorageTypes();
        this.inputDataStart = -1;
        this.inputDataLength = split.getLength();
        this.startOffset = startOffset;
        setIsCached(split.getLocationInfo());
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      }
    }
    
    private void setIsCached(SplitLocationInfo[] splitInfo) {
       this.isCached = new boolean[this.locations.length];
       if (splitInfo == null || splitInfo.length == 0 || splitInfo.length != this.locations.length) {
          for (int i = 0; i < this.isCached.length; ++i)
             this.isCached[i] = false;
       } else {
          for (int i = 0; i < this.isCached.length; ++i)
             this.isCached[i] = splitInfo[i].isInMemory();          
       }
    }
    
    public String[] getLocations() {
      return locations;
    }

    public StorageType[] getStorageTypes() {
      return storageTypes;
    }

    public Path getFilePath() {
      return filePath;
    }
  
    public long getStartOffset() {
      return startOffset;
    }
      
    public long getInputDataStart() {
       return inputDataStart;
    }
     
    public long getInputDataLength() {
      return inputDataLength;
    }
    
    public boolean[] getIsCached() {
       return isCached;
    }
    
    public void setInputDataLocations(String[] locations) {
      this.locations = locations;
    }
    
    public void setInputDataStart(long start) {
       this.inputDataStart = start;
    }

    public void setInputDataLength(long length) {
      this.inputDataLength = length;
    }
    
    public void readFields(DataInput in) throws IOException {
      int len = WritableUtils.readVInt(in);
      locations = new String[len];
      for (int i = 0; i < locations.length; i++) {
        locations[i] = Text.readString(in);
      }
      startOffset = WritableUtils.readVLong(in);
      inputDataStart = WritableUtils.readVLong(in);
      inputDataLength = WritableUtils.readVLong(in);

      len = WritableUtils.readVInt(in);
      storageTypes = new StorageType[len];
      for (int i = 0; i < storageTypes.length; i++) {
        storageTypes[i] = StorageType.parseStorageType(Text.readString(in));
      }

      len = WritableUtils.readVInt(in);
      isCached = new boolean[len];
      for (int i = 0; i < isCached.length; i++) {
         isCached[i] = Boolean.valueOf(Text.readString(in));
      }

      len = WritableUtils.readVInt(in);
      if (len > 0)
        filePath = new Path(Text.readString(in));
    }
  
    public void write(DataOutput out) throws IOException {
      WritableUtils.writeVInt(out, locations.length);

      for (int i = 0; i < locations.length; i++) {
        Text.writeString(out, locations[i]);
      }
      WritableUtils.writeVLong(out, startOffset);
      WritableUtils.writeVLong(out, inputDataStart);
      WritableUtils.writeVLong(out, inputDataLength);

      WritableUtils.writeVInt(out, storageTypes.length);
      for (int i = 0; i < storageTypes.length; i++) {
        Text.writeString(out, storageTypes[i].toString());
      }

      WritableUtils.writeVInt(out, isCached.length);
      for (int i = 0; i < isCached.length; i++) {
        Text.writeString(out, Boolean.toString(isCached[i]));
      }

      if (filePath != null  && !filePath.toString().isEmpty())
      {
        WritableUtils.writeVInt(out, SINGLE_FILE);
        Text.writeString(out, filePath.toString());
      }
      else
        WritableUtils.writeVInt(out, 0);
    }
    
    @Override
    public String toString() {
      StringBuffer buf = new StringBuffer();
      buf.append("data-size : " + inputDataLength + "\n");
      buf.append("start-offset : " + startOffset + "\n");
      buf.append("locations : " + "\n");
      for (String loc : locations) {
        buf.append("  " + loc + "\n");
      }
      return buf.toString();
    }

    public String toOneString() {
       StringBuffer buf = new StringBuffer();
       buf.append("path : " + ((filePath == null) ? "NULL" : filePath) + " ");
       buf.append("data-size : " + inputDataLength + " ");
       buf.append("start-offset : " + startOffset + " ");
       buf.append("locations : ");
       for (String loc : locations) {
         buf.append(loc + " ");
       }
       buf.append("types : ");
       for (StorageType loc : storageTypes) {
         buf.append(loc + " ");
       }
       buf.append("iscached : ");
       for (boolean loc : isCached) {
         buf.append(loc + " ");
       }
       return buf.toString();
    }
}
  /**
   * This represents the meta information about the task split that the 
   * JobTracker creates
   */
  public static class TaskSplitMetaInfo {
    private TaskSplitIndex splitIndex;
    private long inputDataStart;
    private long inputDataLength;
    private String[] locations;
    private Path filePath;
    private StorageType[] storageTypes;
    private boolean[] isCached;

    public TaskSplitMetaInfo(){
      this.splitIndex = new TaskSplitIndex();
      this.locations = new String[0];
      this.storageTypes = new StorageType[0];
      this.filePath = null;
      this.isCached = new boolean[0];
      this.inputDataStart = -1;
      this.inputDataLength = 0;
    }
    public TaskSplitMetaInfo(TaskSplitIndex splitIndex, String[] locations,
                             StorageType[] storageTypes, boolean[] isCached,
                             Path filePath, long inputDataStart, long inputDataLength) {
      this.splitIndex = splitIndex;
      this.locations = locations;
      this.storageTypes = storageTypes;
      this.filePath = filePath;
      this.inputDataStart = inputDataStart;
      this.inputDataLength = inputDataLength;
      this.isCached = isCached;
      
      if (LOG.isDebugEnabled())
         LOG.debug("TaskSplitMetaInfo: " + toOneString());
    }
    public TaskSplitMetaInfo(TaskSplitIndex splitIndex, String[] locations,
        long inputDataLength) {
      this.splitIndex = splitIndex;
      this.locations = locations;
      this.inputDataStart = -1;
      this.inputDataLength = inputDataLength;
    }
    public TaskSplitMetaInfo(InputSplit split, long startOffset) 
    throws InterruptedException, IOException {
      this(new TaskSplitIndex("", startOffset), split.getLocations(), 
          split.getLength());
    }
    
    public TaskSplitMetaInfo(String[] locations, long startOffset, 
        long inputDataLength) {
      this(new TaskSplitIndex("",startOffset), locations, inputDataLength);
    }

    public TaskSplitIndex getSplitIndex() {
      return splitIndex;
    }
    
    public String getSplitLocation() {
      return splitIndex.getSplitLocation();
    }
    public long getInputDataStart() {
       return inputDataStart;
     }
    public long getInputDataLength() {
      return inputDataLength;
    }
    public String[] getLocations() {
      return locations;
    }
    public StorageType[] getStorageTypes() {
      return storageTypes;
    }
    public boolean[] getIsCached() {
       return isCached;
    }
    public Path getFilePath() {
      return filePath;
    }
    
    public boolean hasAnyCached() {
      if (isCached == null)
         return false;
      
      for (int i = 0; i < isCached.length; ++i)
         if (isCached[i])
            return true;
      
      return false;
   }
    
    public boolean hasAnyCachedOrMemory() {
       if (isCached != null) {
          for (int i = 0; i < isCached.length; ++i)
             if (isCached[i])
                return true;
       }
       
       if (storageTypes != null) {
          for (int i = 0; i < storageTypes.length; ++i)
             if (storageTypes[i] == StorageType.MEMORY)
                return true;
       }
       
       return false;
    }

    public String toOneString() {
       StringBuffer buf = new StringBuffer();
       buf.append("path : " + ((filePath == null) ? "NULL" : filePath) + " ");
       buf.append("data-size : " + inputDataLength + " ");
       buf.append("locations : ");
       for (String loc : locations) {
         buf.append(loc + " ");
       }
       buf.append("types : ");
       for (StorageType loc : storageTypes) {
         buf.append(loc + " ");
       }
       buf.append("iscached : ");
       for (boolean loc : isCached) {
         buf.append(loc + " ");
       }
       return buf.toString();
    }

  }
  
  /**
   * This represents the meta information about the task split that the 
   * task gets
   */
  public static class TaskSplitIndex {
    private String splitLocation;
    private long startOffset;
    public TaskSplitIndex(){
      this("", 0);
    }
    public TaskSplitIndex(String splitLocation, long startOffset) {
      this.splitLocation = splitLocation;
      this.startOffset = startOffset;
    }
    public long getStartOffset() {
      return startOffset;
    }
    public String getSplitLocation() {
      return splitLocation;
    }
    public void readFields(DataInput in) throws IOException {
      splitLocation = Text.readString(in);
      startOffset = WritableUtils.readVLong(in);
    }
    public void write(DataOutput out) throws IOException {
      Text.writeString(out, splitLocation);
      WritableUtils.writeVLong(out, startOffset);
    }
  }
}
