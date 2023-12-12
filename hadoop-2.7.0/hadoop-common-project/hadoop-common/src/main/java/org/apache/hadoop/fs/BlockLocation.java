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
package org.apache.hadoop.fs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.util.Arrays;

/**
 * Represents the network location of a block, information about the hosts
 * that contain block replicas, and other block metadata (E.g. the file
 * offset associated with the block, length, whether it is corrupt, etc).
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class BlockLocation {
  private String[] hosts; // Datanode hostnames
  private String[] cachedHosts; // Datanode hostnames with a cached replica
  private String[] names; // Datanode IP:xferPort for accessing the block
  private String[] topologyPaths; // Full path name in network topology
  private StorageType[] storageTypes; // Storage Types of hosts locations
  private long offset;  // Offset of the block in the file
  private long length;
  private boolean corrupt;

  private static final String[] EMPTY_STR_ARRAY = new String[0];
  private static final StorageType[] EMPTY_TYPE_ARRAY = new StorageType[0];
  
  //---------------------------------------
  //Added for Octopus Prefetching by Elena
  //---------------------------------------
  private String[] storageIDs;
  private String[] pendingCachedHosts; // Datanode hostnames with a pending cached replica
  //---------------------------------------
  
  /**
   * Default Constructor
   */
  public BlockLocation() {
    this(EMPTY_STR_ARRAY, EMPTY_STR_ARRAY, 0L, 0L);
  }

  /**
   * Copy constructor
   */
  public BlockLocation(BlockLocation that) {
    this.hosts = that.hosts;
    this.storageTypes = that.storageTypes;
    this.cachedHosts = that.cachedHosts;
    this.names = that.names;
    this.topologyPaths = that.topologyPaths;
    this.offset = that.offset;
    this.length = that.length;
    this.corrupt = that.corrupt;
    //---------------------------------------
    //Added for Octopus Prefetching by Elena
    //---------------------------------------
    this.storageIDs = that.storageIDs;
    this.pendingCachedHosts = that.pendingCachedHosts;
    //---------------------------------------
    
    
  }

  /**
   * Constructor with host, name, offset and length
   */
  public BlockLocation(String[] names, String[] hosts, long offset, 
                       long length) {
    this(names, hosts, offset, length, false);
  }

  /**
   * Constructor with host, name, offset, length and corrupt flag
   */
  public BlockLocation(String[] names, String[] hosts, long offset, 
                       long length, boolean corrupt) {
    this(names, hosts, null, offset, length, corrupt);
  }

  /**
   * Constructor with host, name, network topology, offset and length
   */
  public BlockLocation(String[] names, String[] hosts, String[] topologyPaths,
                       long offset, long length) {
    this(names, hosts, topologyPaths, offset, length, false);
  }

  /**
   * Constructor with host, name, network topology, offset, length 
   * and corrupt flag
   */
  public BlockLocation(String[] names, String[] hosts, String[] topologyPaths,
                       long offset, long length, boolean corrupt) {
    this(names, hosts, null, topologyPaths, offset, length, corrupt);
  }

  
  public BlockLocation(String[] names, String[] hosts, String[] cachedHosts,
      String[] topologyPaths, long offset, long length, boolean corrupt) {
    if (names == null) {
      this.names = EMPTY_STR_ARRAY;
    } else {
      this.names = names;
    }
    if (hosts == null) {
      this.hosts = EMPTY_STR_ARRAY;
      this.storageTypes = EMPTY_TYPE_ARRAY;
    } else {
      this.hosts = hosts;
      this.storageTypes = new StorageType[hosts.length];
      Arrays.fill(this.storageTypes, StorageType.DEFAULT);
    }
    if (cachedHosts == null) {
      this.cachedHosts = EMPTY_STR_ARRAY;
    } else {
      this.cachedHosts = cachedHosts;
    }
    if (topologyPaths == null) {
      this.topologyPaths = EMPTY_STR_ARRAY;
    } else {
      this.topologyPaths = topologyPaths;
    }
    this.offset = offset;
    this.length = length;
    this.corrupt = corrupt;
    
    this.storageIDs = EMPTY_STR_ARRAY;
    this.pendingCachedHosts = EMPTY_STR_ARRAY;
  }

  public BlockLocation(String[] names, String[] hosts, StorageType[] storageTypes, String[] cachedHosts,
                       String[] topologyPaths, long offset, long length, boolean corrupt) {
    if (names == null) {
      this.names = EMPTY_STR_ARRAY;
    } else {
      this.names = names;
    }
    if (hosts == null) {
      this.hosts = EMPTY_STR_ARRAY;
    } else {
      this.hosts = hosts;
    }
    if (cachedHosts == null) {
      this.cachedHosts = EMPTY_STR_ARRAY;
    } else {
      this.cachedHosts = cachedHosts;
    }
    if (topologyPaths == null) {
      this.topologyPaths = EMPTY_STR_ARRAY;
    } else {
      this.topologyPaths = topologyPaths;
    }
    this.storageTypes = storageTypes;
    this.offset = offset;
    this.length = length;
    this.corrupt = corrupt;
    
    this.storageIDs = EMPTY_STR_ARRAY;
    this.pendingCachedHosts = EMPTY_STR_ARRAY;
  }
  
  //---------------------------------------
  //Added for Octopus Prefetching by Elena
  //---------------------------------------
  public BlockLocation(String[] names, String[] hosts, StorageType[] storageTypes, String[] cachedHosts,
          String[] topologyPaths, long offset, long length, boolean corrupt,
          String[] storageIDs, String[] pendingCachedHosts) {
	  if (names == null) {
		  this.names = EMPTY_STR_ARRAY;
	  } else {
		  this.names = names;
	  }
	  if (hosts == null) {
		  this.hosts = EMPTY_STR_ARRAY;
	  } else {
		  this.hosts = hosts;
	  }
	  if (cachedHosts == null) {
		  this.cachedHosts = EMPTY_STR_ARRAY;
	  } else {
		  this.cachedHosts = cachedHosts;
	  }
	  if (topologyPaths == null) {
		  this.topologyPaths = EMPTY_STR_ARRAY;
	  } else {
		  this.topologyPaths = topologyPaths;
	  }
	  this.storageTypes = storageTypes;
	  this.offset = offset;
	  this.length = length;
	  this.corrupt = corrupt;
	  
     if (storageIDs == null) {
        this.storageIDs = EMPTY_STR_ARRAY;
     } else {
        this.storageIDs = storageIDs;
     }
     
	  if (pendingCachedHosts == null) {
		  this.pendingCachedHosts = EMPTY_STR_ARRAY;
	  } else {
		  this.pendingCachedHosts = pendingCachedHosts;
	  }
  }
  //---------------------------------------
  
  /**
   * Get the list of hosts (hostname) hosting this block
   */
  public String[] getHosts() {
    return hosts;
  }

  /**
   * Get the list of hosts storage types hosting this block
   */
  public StorageType[] getStorageTypes() {
    return storageTypes;
  }

  /**
   * Get the list of hosts (hostname) hosting a cached replica of the block
   */
  public String[] getCachedHosts() {
   return cachedHosts;
  }

  /**
   * Get the list of names (IP:xferPort) hosting this block
   */
  public String[] getNames() {
    return names;
  }

  /**
   * Get the list of network topology paths for each of the hosts.
   * The last component of the path is the "name" (IP:xferPort).
   */
  public String[] getTopologyPaths() {
    return topologyPaths;
  }
  
  /**
   * Get the start offset of file associated with this block
   */
  public long getOffset() {
    return offset;
  }
  
  /**
   * Get the length of the block
   */
  public long getLength() {
    return length;
  }

  /**
   * Get the corrupt flag.
   */
  public boolean isCorrupt() {
    return corrupt;
  }

  //---------------------------------------
  //Added for Octopus Prefetching by Elena
  //---------------------------------------
  /**
   * Get the list of storage IDs hosting the replicas of this block
   */
  public String[] getStorageIDs() {
   return storageIDs;
  }

  /**
   * Get the list of hosts (hostname) hosting a pending cached replica of the block
   */
  public String[] getPendingCachedHosts() {
   return pendingCachedHosts;
  }
  //---------------------------------------
  
  /**
   * Set the start offset of file associated with this block
   */
  public void setOffset(long offset) {
    this.offset = offset;
  }

  /**
   * Set the length of block
   */
  public void setLength(long length) {
    this.length = length;
  }

  /**
   * Set the corrupt flag.
   */
  public void setCorrupt(boolean corrupt) {
    this.corrupt = corrupt;
  }

  /**
   * Set the hosts hosting this block
   */
  public void setHosts(String[] hosts) {
    if (hosts == null) {
      this.hosts = EMPTY_STR_ARRAY;
    } else {
      this.hosts = hosts;
    }
  }

  /**
   * Set the hosts hosting a cached replica of this block
   */
  public void setCachedHosts(String[] cachedHosts) {
    if (cachedHosts == null) {
      this.cachedHosts = EMPTY_STR_ARRAY;
    } else {
      this.cachedHosts = cachedHosts;
    }
  }

  /**
   * Set the names (host:port) hosting this block
   */
  public void setNames(String[] names) {
    if (names == null) {
      this.names = EMPTY_STR_ARRAY;
    } else {
      this.names = names;
    }
  }

  /**
   * Set the network topology paths of the hosts
   */
  public void setTopologyPaths(String[] topologyPaths) {
    if (topologyPaths == null) {
      this.topologyPaths = EMPTY_STR_ARRAY;
    } else {
      this.topologyPaths = topologyPaths;
    }
  }

  //---------------------------------------
  //Added for Octopus Prefetching by Elena
  //---------------------------------------
  /**
   * Set the hosts hosting a pending cached replica of this block
   */
  public void setPendingCachedHosts(String[] pendingCachedHosts) {
    if (pendingCachedHosts == null) {
      this.pendingCachedHosts = EMPTY_STR_ARRAY;
    } else {
      this.pendingCachedHosts = pendingCachedHosts;
    }
  }
  //---------------------------------------
  
  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append(offset);
    result.append(',');
    result.append(length);
    if (corrupt) {
      result.append("(corrupt)");
    }
    for(String h: hosts) {
      result.append(',');
      result.append(h);
    }
    return result.toString();
  }
}