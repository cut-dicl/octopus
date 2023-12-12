/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;

/**
 * Status information on the current state of the Map-Reduce cluster.
 * 
 * <p><code>ClusterMetrics</code> provides clients with information such as:
 * <ol>
 *   <li>
 *   Size of the cluster.  
 *   </li>
 *   <li>
 *   Number of blacklisted and decommissioned trackers.  
 *   </li>
 *   <li>
 *   Slot capacity of the cluster. 
 *   </li>
 *   <li>
 *   The number of currently occupied/reserved map and reduce slots.
 *   </li>
 *   <li>
 *   The number of currently running map and reduce tasks.
 *   </li>
 *   <li>
 *   The number of job submissions.
 *   </li>
 * </ol>
 * 
 * <p>Clients can query for the latest <code>ClusterMetrics</code>, via 
 * {@link Cluster#getClusterStatus()}.</p>
 * 
 * @see Cluster
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ClusterMetrics implements Writable {
  private int runningMaps;
  private int runningReduces;
  private int occupiedMapSlots;
  private int occupiedReduceSlots;
  private int reservedMapSlots;
  private int reservedReduceSlots;
  private int totalMapSlots;
  private int totalReduceSlots;
  private int totalJobSubmissions;
  private int numTrackers;
  private int numBlacklistedTrackers;
  private int numGraylistedTrackers;
  private int numDecommissionedTrackers;
  
  //---------------------------------------
  //Added for Octopus Prefetching by Elena
  //---------------------------------------
  private int allocatedMemory;
  private int availableMemory;
  private int reservedMemory;
  private int allocatedCores;
  private int availableCores;
  private int reservedCores;
  private int appsRunning;
  private int appsPending;
  //---------------------------------------

  public ClusterMetrics() {
  }
  
  public ClusterMetrics(int runningMaps, int runningReduces,
      int occupiedMapSlots, int occupiedReduceSlots, int reservedMapSlots,
      int reservedReduceSlots, int mapSlots, int reduceSlots,
      int totalJobSubmissions, int numTrackers, int numBlacklistedTrackers,
      int numDecommissionedNodes) {
    this(runningMaps, runningReduces, occupiedMapSlots, occupiedReduceSlots,
      reservedMapSlots, reservedReduceSlots, mapSlots, reduceSlots,
      totalJobSubmissions, numTrackers, numBlacklistedTrackers, 0,
      numDecommissionedNodes);
  }

  public ClusterMetrics(int runningMaps, int runningReduces,
      int occupiedMapSlots, int occupiedReduceSlots, int reservedMapSlots,
      int reservedReduceSlots, int mapSlots, int reduceSlots,
      int totalJobSubmissions, int numTrackers, int numBlacklistedTrackers,
      int numGraylistedTrackers, int numDecommissionedNodes) {
    this.runningMaps = runningMaps;
    this.runningReduces = runningReduces;
    this.occupiedMapSlots = occupiedMapSlots;
    this.occupiedReduceSlots = occupiedReduceSlots;
    this.reservedMapSlots = reservedMapSlots;
    this.reservedReduceSlots = reservedReduceSlots;
    this.totalMapSlots = mapSlots;
    this.totalReduceSlots = reduceSlots;
    this.totalJobSubmissions = totalJobSubmissions;
    this.numTrackers = numTrackers;
    this.numBlacklistedTrackers = numBlacklistedTrackers;
    this.numGraylistedTrackers = numGraylistedTrackers;
    this.numDecommissionedTrackers = numDecommissionedNodes;
  }

  //---------------------------------------
  //Added for Octopus Prefetching by Elena
  //---------------------------------------
  public ClusterMetrics(int runningMaps, int runningReduces,
	      int occupiedMapSlots, int occupiedReduceSlots, int reservedMapSlots,
	      int reservedReduceSlots, int mapSlots, int reduceSlots,
	      int totalJobSubmissions, int numTrackers, int numBlacklistedTrackers,
	      int numGraylistedTrackers, int numDecommissionedNodes, int allocatedMemory, 
	      int availableMemory, int reservedMemory, int allocatedCores, int availableCores, 
	      int reservedCores, int appsRunning, int appsPending) {
	    this.runningMaps = runningMaps;
	    this.runningReduces = runningReduces;
	    this.occupiedMapSlots = occupiedMapSlots;
	    this.occupiedReduceSlots = occupiedReduceSlots;
	    this.reservedMapSlots = reservedMapSlots;
	    this.reservedReduceSlots = reservedReduceSlots;
	    this.totalMapSlots = mapSlots;
	    this.totalReduceSlots = reduceSlots;
	    this.totalJobSubmissions = totalJobSubmissions;
	    this.numTrackers = numTrackers;
	    this.numBlacklistedTrackers = numBlacklistedTrackers;
	    this.numGraylistedTrackers = numGraylistedTrackers;
	    this.numDecommissionedTrackers = numDecommissionedNodes;
	    this.allocatedMemory = allocatedMemory;
	    this.availableMemory = availableMemory;
	    this.reservedMemory = reservedMemory;
	    this.allocatedCores = allocatedCores;
	    this.availableCores = availableCores;
	    this.reservedCores = reservedCores;
	    this.appsRunning = appsRunning;
	    this.appsPending = appsPending;
	  }
  //---------------------------------------
  
  /**
   * Get the number of running map tasks in the cluster.
   * 
   * @return running maps
   */
  public int getRunningMaps() {
    return runningMaps;
  }
  
  /**
   * Get the number of running reduce tasks in the cluster.
   * 
   * @return running reduces
   */
  public int getRunningReduces() {
    return runningReduces;
  }
  
  /**
   * Get number of occupied map slots in the cluster.
   * 
   * @return occupied map slot count
   */
  public int getOccupiedMapSlots() { 
    return occupiedMapSlots;
  }
  
  /**
   * Get the number of occupied reduce slots in the cluster.
   * 
   * @return occupied reduce slot count
   */
  public int getOccupiedReduceSlots() { 
    return occupiedReduceSlots; 
  }

  /**
   * Get number of reserved map slots in the cluster.
   * 
   * @return reserved map slot count
   */
  public int getReservedMapSlots() { 
    return reservedMapSlots;
  }
  
  /**
   * Get the number of reserved reduce slots in the cluster.
   * 
   * @return reserved reduce slot count
   */
  public int getReservedReduceSlots() { 
    return reservedReduceSlots; 
  }

  /**
   * Get the total number of map slots in the cluster.
   * 
   * @return map slot capacity
   */
  public int getMapSlotCapacity() {
    return totalMapSlots;
  }
  
  /**
   * Get the total number of reduce slots in the cluster.
   * 
   * @return reduce slot capacity
   */
  public int getReduceSlotCapacity() {
    return totalReduceSlots;
  }
  
  /**
   * Get the total number of job submissions in the cluster.
   * 
   * @return total number of job submissions
   */
  public int getTotalJobSubmissions() {
    return totalJobSubmissions;
  }
  
  /**
   * Get the number of active trackers in the cluster.
   * 
   * @return active tracker count.
   */
  public int getTaskTrackerCount() {
    return numTrackers;
  }
  
  /**
   * Get the number of blacklisted trackers in the cluster.
   * 
   * @return blacklisted tracker count
   */
  public int getBlackListedTaskTrackerCount() {
    return numBlacklistedTrackers;
  }
  
  /**
   * Get the number of graylisted trackers in the cluster.
   * 
   * @return graylisted tracker count
   */
  public int getGrayListedTaskTrackerCount() {
    return numGraylistedTrackers;
  }
  
  /**
   * Get the number of decommissioned trackers in the cluster.
   * 
   * @return decommissioned tracker count
   */
  public int getDecommissionedTaskTrackerCount() {
    return numDecommissionedTrackers;
  }
  
  //---------------------------------------
  //Added for Octopus Prefetching by Elena
  //---------------------------------------
  
  /**
   * Get the number of allocated memory in the cluster.
   * 
   * @return allocated memory
   */
  public int getAllocatedMemory() {
    return allocatedMemory;
  }
  
  /**
   * Get the number of available memory in the cluster.
   * 
   * @return available memory
   */
  public int getAvailableMemory() {
    return availableMemory;
  }
  
  /**
   * Get the number of reserved memory in the cluster.
   * 
   * @return reserved memory
   */
  public int getReservedMemory() {
    return reservedMemory;
  }
  
  /**
   * Get the number of allocated cores in the cluster.
   * 
   * @return allocated cores
   */
  public int getAllocatedCores() {
    return allocatedCores;
  }
  
  /**
   * Get the number of available cores in the cluster.
   * 
   * @return available cores
   */
  public int getAvailableCores() {
    return availableCores;
  }
  
  /**
   * Get the number of reserved cores in the cluster.
   * 
   * @return reserved cores
   */
  public int getReservedCores() {
    return reservedCores;
  }
  
  /**
   * Get the number of running applications in the cluster.
   * 
   * @return running applications
   */
  public int getAppsRunning() {
    return appsRunning;
  }
  
  /**
   * Get the number of pending applications in the cluster.
   * 
   * @return pending applications
   */
  public int getAppsPending() {
    return appsPending;
  }
  //---------------------------------------

  @Override
  public void readFields(DataInput in) throws IOException {
    runningMaps = in.readInt();
    runningReduces = in.readInt();
    occupiedMapSlots = in.readInt();
    occupiedReduceSlots = in.readInt();
    reservedMapSlots = in.readInt();
    reservedReduceSlots = in.readInt();
    totalMapSlots = in.readInt();
    totalReduceSlots = in.readInt();
    totalJobSubmissions = in.readInt();
    numTrackers = in.readInt();
    numBlacklistedTrackers = in.readInt();
    numGraylistedTrackers = in.readInt();
    numDecommissionedTrackers = in.readInt();
    
    //---------------------------------------
    //Added for Octopus Prefetching by Elena
    //---------------------------------------    
    allocatedMemory = in.readInt();
    availableMemory = in.readInt();
    reservedMemory = in.readInt();
    allocatedCores = in.readInt();
    availableCores = in.readInt();
    reservedCores = in.readInt();
    appsRunning = in.readInt();
    appsPending = in.readInt();
    //---------------------------------------
    
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(runningMaps);
    out.writeInt(runningReduces);
    out.writeInt(occupiedMapSlots);
    out.writeInt(occupiedReduceSlots);
    out.writeInt(reservedMapSlots);
    out.writeInt(reservedReduceSlots);
    out.writeInt(totalMapSlots);
    out.writeInt(totalReduceSlots);
    out.writeInt(totalJobSubmissions);
    out.writeInt(numTrackers);
    out.writeInt(numBlacklistedTrackers);
    out.writeInt(numGraylistedTrackers);
    out.writeInt(numDecommissionedTrackers);
    
    //---------------------------------------
    //Added for Octopus Prefetching by Elena
    //---------------------------------------
    out.writeInt(allocatedMemory);
    out.writeInt(availableMemory);
    out.writeInt(reservedMemory);
    out.writeInt(allocatedCores);
    out.writeInt(availableCores);
    out.writeInt(reservedCores);
    out.writeInt(appsRunning);
    out.writeInt(appsPending);
    //---------------------------------------
  }

}
