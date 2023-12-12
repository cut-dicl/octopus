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
package org.apache.hadoop.hdfs.protocol;

import java.util.Arrays;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.security.token.Token;

/**
 * Associates a block with the Datanodes that contain its replicas
 * and other block metadata (E.g. the file offset associated with this
 * block, whether it is corrupt, a location is cached in memory,
 * security token, etc).
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class LocatedBlock {

  private final ExtendedBlock b;
  private long offset;  // offset of the first byte of the block in the file
  private final DatanodeInfoWithStorage[] locs;
  /** Cached storage ID for each replica */
  private String[] storageIDs;
  /** Cached storage type for each replica, if reported. */
  private StorageType[] storageTypes;
  // corrupt flag is true if all of the replicas of a block are corrupt.
  // else false. If block has few corrupt replicas, they are filtered and 
  // their locations are not part of this object
  private boolean corrupt;
  private Token<BlockTokenIdentifier> blockToken = new Token<BlockTokenIdentifier>();
  /** List of cached datanode locations */
  private DatanodeInfoWithStorage[] cachedLocs;

  /** All locs including cached locations **/
  private DatanodeInfoWithStorage[] allLocs;
  private String[] allStorageIDs;
  private StorageType[] allStorageTypes;
  
  //---------------------------------------
  //Added for Octopus Prefetching by Elena
  //---------------------------------------
  private DatanodeInfoWithStorage[] pendingCachedLocs;
  //---------------------------------------
  
  // Used when there are no locations
  private static final DatanodeInfoWithStorage[] EMPTY_LOCS =
      new DatanodeInfoWithStorage[0];

  public LocatedBlock(ExtendedBlock b, DatanodeInfo[] locs) {
    this(b, locs, -1, false); // startOffset is unknown
  }

  //-----------------------------------------
  //Changes for Octopus Prefetching by Elena
  //-----------------------------------------
  public LocatedBlock(ExtendedBlock b, DatanodeInfo[] locs, long startOffset, 
                      boolean corrupt) {
    this(b, locs, null, null, startOffset, corrupt, EMPTY_LOCS, EMPTY_LOCS);
  }

  public LocatedBlock(ExtendedBlock b, DatanodeStorageInfo[] storages) {
    this(b, storages, -1, false); // startOffset is unknown
  }

  public LocatedBlock(ExtendedBlock b, DatanodeInfo[] locs,
                      String[] storageIDs, StorageType[] storageTypes) {
    this(b, locs, storageIDs, storageTypes, -1, false, EMPTY_LOCS, EMPTY_LOCS);
  }

  public LocatedBlock(ExtendedBlock b, DatanodeStorageInfo[] storages,
      long startOffset, boolean corrupt) {
    this(b, DatanodeStorageInfo.toDatanodeInfos(storages),
        DatanodeStorageInfo.toStorageIDs(storages),
        DatanodeStorageInfo.toStorageTypes(storages),
        startOffset, corrupt, EMPTY_LOCS, EMPTY_LOCS); // startOffset is unknown
  }

  public LocatedBlock(ExtendedBlock b, DatanodeInfo[] locs, String[] storageIDs,
                      StorageType[] storageTypes, long startOffset,
                      boolean corrupt, DatanodeInfoWithStorage[] cachedLocs, 
                      DatanodeInfoWithStorage[] pendingCachedLocs) {
    this.b = b;
    this.offset = startOffset;
    this.corrupt = corrupt;
    if (locs==null) {
      this.locs = EMPTY_LOCS;
    } else {
      this.locs = new DatanodeInfoWithStorage[locs.length];
      for(int i = 0; i < locs.length; i++) {
        DatanodeInfo di = locs[i];
        DatanodeInfoWithStorage storage = new DatanodeInfoWithStorage(di,
            storageIDs != null ? storageIDs[i] : null,
            storageTypes != null ? storageTypes[i] : null);
        this.locs[i] = storage;
      }
    }
    this.storageIDs = storageIDs;
    this.storageTypes = storageTypes;

    addCachedLoc(cachedLocs);
    addPendingCachedLoc(pendingCachedLocs);
  }
  
  public Token<BlockTokenIdentifier> getBlockToken() {
    return blockToken;
  }

  public void setBlockToken(Token<BlockTokenIdentifier> token) {
    this.blockToken = token;
  }

  public ExtendedBlock getBlock() {
    return b;
  }
  
  /**
   * @return the first location if any or null
   */
  public DatanodeInfoWithStorage getFirstLocation() {
     if (allLocs.length > 0)
        return allLocs[0];
     else
        return null;
  }

  /**
   * Returns the locations associated with this block. The returned array is not
   * expected to be modified. If it is, caller must immediately invoke
   * {@link org.apache.hadoop.hdfs.protocol.LocatedBlock#updateCachedStorageInfo}
   * to update the cached Storage ID/Type arrays.
   */
  public DatanodeInfo[] getLocations() {
    return locs;
  }

  public StorageType[] getStorageTypes() {
    return storageTypes;
  }
  
  public String[] getStorageIDs() {
    return storageIDs;
  }

  public DatanodeInfoWithStorage[] getAllLocations() {
     return allLocs;
   }
  
  public StorageType[] getAllStorageTypes() {
     return allStorageTypes;
   }
   
   public String[] getAllStorageIDs() {
     return allStorageIDs;
   }

   /**
   * Updates the cached StorageID and StorageType information. Must be
   * called when the "all" locations array is modified.
   */
  public void updateCachedStorageInfo() {
    if (allStorageIDs != null) {
      for(int i = 0; i < allLocs.length; i++) {
        allStorageIDs[i] = allLocs[i].getStorageID();
      }
    }
    if (allStorageTypes != null) {
      for(int i = 0; i < allLocs.length; i++) {
        allStorageTypes[i] = allLocs[i].getStorageType();
      }
    }
    
    if (allLocs != locs) {
       // We need to update locs and cachedLocs with the new sort order
       boolean[] isCached = new boolean[allLocs.length];
       for (int i = 0; i < allLocs.length; ++i) {
          // Check is this loc is cached. CachedLocs is small so this
          // is probably more efficient than building a hash
          isCached[i] = false;
          for (int j = 0; j < cachedLocs.length; ++j) {
             if (cachedLocs[j] == allLocs[i]) {
                isCached[i] = true;
                break;
             }
          }
       }
       
       // Split all locs into regular and cached locs
       int l = 0, c = 0;
       for (int i = 0; i < allLocs.length; ++i) {
          if (isCached[i] == false) {
             locs[l] = allLocs[i];
             if (storageIDs != null) 
                storageIDs[l] = locs[l].getStorageID();
             if (storageTypes != null) 
                storageTypes[l] = locs[l].getStorageType();
             ++l;
          } else {
             cachedLocs[c] = allLocs[i];
             ++c;
          }
       }
    }
  }

  /**
   * Updates the "all" locations array with the provided sortness.
   * Assumptions: regular and cached locations are already sorted.
   * The provided array shows only the global sortness of the two lists.
   * 
   * @param sortedStorageIDs
   */
  public void updateAllSortedLocations(String[] sortedStorageIDs) {
     if (sortedStorageIDs == null 
           || sortedStorageIDs.length == 0
           || sortedStorageIDs.length == locs.length)
        return;
     
     assert sortedStorageIDs.length == allLocs.length;
     
     // Find the correct locations from the two lists and add it to allLocs
     int l = 0, c = 0;
     DatanodeInfoWithStorage loc;
     for (int i = 0; i < sortedStorageIDs.length; ++i) {
        if (c < cachedLocs.length 
              && cachedLocs[c].getStorageID().equals(sortedStorageIDs[i])) {
           loc = cachedLocs[c];
           ++c;
        } else {
           loc = locs[l];
           ++l;
        }
        
        allLocs[i] = loc;
        allStorageIDs[i] = loc.getStorageID();
        allStorageTypes[i] = loc.getStorageType();
     }
  }
  
  public long getStartOffset() {
    return offset;
  }
  
  public long getBlockSize() {
    return b.getNumBytes();
  }

  void setStartOffset(long value) {
    this.offset = value;
  }

  void setCorrupt(boolean corrupt) {
    this.corrupt = corrupt;
  }
  
  public boolean isCorrupt() {
    return this.corrupt;
  }

  /**
   * Add the location of cached replicas of the block.
   * 
   * @param locs of datanodes with the cached replica
   */
  public void addCachedLoc(DatanodeInfoWithStorage[] cachedLocs) { 
     if (cachedLocs == null || cachedLocs.length == 0) {
        // No cached locs, only regular locs
        this.cachedLocs = EMPTY_LOCS;
        this.allLocs = this.locs;
        this.allStorageIDs = this.storageIDs;
        this.allStorageTypes = this.storageTypes;
        return;
     } else {
        this.cachedLocs = cachedLocs;
     }

     // Add the regular and cached locations together
     int allLenth = locs.length + cachedLocs.length;
     allLocs = new DatanodeInfoWithStorage[allLenth];
     
     // Add the regular locations
     int i = 0;
     while (i < locs.length) {
        allLocs[i] = locs[i];
        ++i;
     }
     
     // Add the new cached locations
     int j = 0;
     while (i < allLocs.length) {
        allLocs[i] = cachedLocs[j];
        ++i;
        ++j;
     }
     
     // Set the storage ids and types
     allStorageIDs = new String[allLenth];
     allStorageTypes = new StorageType[allLenth];
     for (int k = 0; k < allLocs.length; ++k) {
        allStorageIDs[k] = allLocs[k].getStorageID();
        allStorageTypes[k] = allLocs[k].getStorageType();
     }
  }

  /**
   * @return Datanodes with a cached block replica
   */
  public DatanodeInfoWithStorage[] getCachedLocations() {
    return cachedLocs;
  }

  //---------------------------------------
  //Added for Octopus Prefetching by Elena
  //--------------------------------------- 
  /**
   * Add the location of pending cached replicas of the block.
   * 
   * @param locs of datanodes with the pending cached replica
   */
  public void addPendingCachedLoc(DatanodeInfoWithStorage[] pendingCachedLocs) { 
	  
     if (pendingCachedLocs == null || pendingCachedLocs.length == 0) {
        // No pending cached locs
        this.pendingCachedLocs = EMPTY_LOCS;
     } else {
        this.pendingCachedLocs = pendingCachedLocs;
     }
  }
  
  /**
   * @return Datanodes with a pending cached block replica
   */
  public DatanodeInfoWithStorage[] getPendingCachedLocations() {
    return pendingCachedLocs;
  }
  //---------------------------------------
  
  
  @Override
  public String toString() {
    return getClass().getSimpleName() + "{" + b
        + "; getBlockSize()=" + getBlockSize()
        + "; corrupt=" + corrupt
        + "; offset=" + offset
        + "; locs=" + Arrays.asList(locs)
        + "}";
  }
}

