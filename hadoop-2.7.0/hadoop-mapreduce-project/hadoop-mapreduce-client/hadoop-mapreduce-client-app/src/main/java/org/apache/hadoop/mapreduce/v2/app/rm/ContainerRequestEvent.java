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

package org.apache.hadoop.mapreduce.v2.app.rm;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.mapreduce.split.JobSplit;
import org.apache.hadoop.mapreduce.split.SplitLocationDetails;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.yarn.api.records.Resource;


public class ContainerRequestEvent extends ContainerAllocatorEvent {
  
  private final Resource capability;
  private final String[] hosts; // unique
  private final String[] racks; // unique
  private final SplitLocationDetails[] splitLocs;
  private final Path filepath;
  private final JobSplit.TaskSplitMetaInfo splitMetaInfo;
  private boolean earlierAttemptFailed = false;

  public ContainerRequestEvent(TaskAttemptId attemptID, 
      Resource capability,
      String[] hosts, String[] racks) {
    super(attemptID, ContainerAllocator.EventType.CONTAINER_REQ);
    this.capability = capability;
    this.hosts = hosts;
    this.racks = racks;
    this.filepath = null;
    this.splitMetaInfo = null;
    
    this.splitLocs = new SplitLocationDetails[hosts.length];
    for (int i = 0; i < splitLocs.length && racks.length > 0; ++i)
       this.splitLocs[i] = new SplitLocationDetails(racks[0], hosts[i], 
             StorageType.DEFAULT);
  }

  public ContainerRequestEvent(TaskAttemptId attemptID,
                               Resource capability,
                               String[] hosts,
                               String[] racks,
                               SplitLocationDetails[] splitLocs,
                               Path filePath,
                               JobSplit.TaskSplitMetaInfo splitMetaInfo) {
    super(attemptID, ContainerAllocator.EventType.CONTAINER_REQ);
    this.capability = capability;
    this.hosts = hosts;
    this.racks = racks;
    this.splitLocs = splitLocs;
    this.filepath = filePath;
    this.splitMetaInfo = splitMetaInfo;
  }
  
  ContainerRequestEvent(TaskAttemptId attemptID, Resource capability) {
    this(attemptID, capability, new String[0], new String[0]);
    this.earlierAttemptFailed = true;
  }
  
  public static ContainerRequestEvent createContainerRequestEventForFailedContainer(
      TaskAttemptId attemptID,
      Resource capability) {
    //ContainerRequest for failed events does not consider rack / node locality?
    return new ContainerRequestEvent(attemptID, capability);
  }

  public Resource getCapability() {
    return capability;
  }

  public String[] getHosts() {
    return hosts;
  }

  public SplitLocationDetails[] getSplitLocationDetails() {
    return splitLocs;
  }

  public Path getFilepath() { return filepath; }

  public String[] getRacks() {
    return racks;
  }
  
  public boolean getEarlierAttemptFailed() {
    return earlierAttemptFailed;
  }

  public JobSplit.TaskSplitMetaInfo getSplitMetaInfo() { return splitMetaInfo; }
}