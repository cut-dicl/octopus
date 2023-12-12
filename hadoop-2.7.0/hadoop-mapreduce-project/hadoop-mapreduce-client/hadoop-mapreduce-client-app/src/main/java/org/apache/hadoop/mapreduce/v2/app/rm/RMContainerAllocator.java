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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.NormalizedResourceEvent;
import org.apache.hadoop.mapreduce.prefetch.MRPrefetcher;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app.job.event.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.api.NMTokenCache;
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException;
import org.apache.hadoop.yarn.exceptions.ApplicationMasterNotRegisteredException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.HungarianAlgorithm;
import org.apache.hadoop.yarn.util.RackResolver;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Allocates the container from the ResourceManager scheduler.
 */
public class RMContainerAllocator extends RMContainerRequestor
    implements ContainerAllocator {

  static final Log LOG = LogFactory.getLog(RMContainerAllocator.class);
  
  private static final int COST_NODE_LOCAL = 0;
  private static final int COST_RACK_LOCAL = 10;
  private static final int COST_OFF_SWITCH = 20;

  public static final 
  float DEFAULT_COMPLETED_MAPS_PERCENT_FOR_REDUCE_SLOWSTART = 0.05f;
  
  static final Priority PRIORITY_FAST_FAIL_MAP;
  static final Priority PRIORITY_REDUCE;
  static final Priority PRIORITY_MAP;

  @VisibleForTesting
  public static final String RAMPDOWN_DIAGNOSTIC = "Reducer preempted "
      + "to make room for pending map attempts";

  private Thread eventHandlingThread;
  private final AtomicBoolean stopped;

  static {
    PRIORITY_FAST_FAIL_MAP = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(Priority.class);
    PRIORITY_FAST_FAIL_MAP.setPriority(5);
    PRIORITY_REDUCE = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(Priority.class);
    PRIORITY_REDUCE.setPriority(10);
    PRIORITY_MAP = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(Priority.class);
    PRIORITY_MAP.setPriority(20);
  }
  
  /*
  Vocabulary Used: 
  pending -> requests which are NOT yet sent to RM
  scheduled -> requests which are sent to RM but not yet assigned
  assigned -> requests which are assigned to a container
  completed -> request corresponding to which container has completed
  
  Lifecycle of map
  scheduled->assigned->completed
  
  Lifecycle of reduce
  pending->scheduled->assigned->completed
  
  Maps are scheduled as soon as their requests are received. Reduces are 
  added to the pending and are ramped up (added to scheduled) based 
  on completed maps and current availability in the cluster.
  */
  
  //reduces which are not yet scheduled
  private final LinkedList<ContainerRequest> pendingReduces = 
    new LinkedList<ContainerRequest>();

  //holds information about the assigned containers to task attempts
  private final AssignedRequests assignedRequests = new AssignedRequests();
  
  //holds scheduled requests to be fulfilled by RM
  private final ScheduledRequests scheduledRequests = new ScheduledRequests();
  
  private int containersAllocated = 0;
  private int containersReleased = 0;
  private int hostLocalAssigned = 0;
  private int rackLocalAssigned = 0;
  private int lastCompletedTasks = 0;
  
  private boolean recalculateReduceSchedule = false;
  private Resource mapResourceRequest = Resources.none();
  private Resource reduceResourceRequest = Resources.none();
  
  private boolean reduceStarted = false;
  private float maxReduceRampupLimit = 0;
  private float maxReducePreemptionLimit = 0;
  /**
   * after this threshold, if the container request is not allocated, it is
   * considered delayed.
   */
  private long allocationDelayThresholdMs = 0;
  private float reduceSlowStart = 0;
  private int maxRunningMaps = 0;
  private int maxRunningReduces = 0;
  private long retryInterval;
  private long retrystartTime;
  private Clock clock;

  @VisibleForTesting
  protected BlockingQueue<ContainerAllocatorEvent> eventQueue
    = new LinkedBlockingQueue<ContainerAllocatorEvent>();

  private ScheduleStats scheduleStats = new ScheduleStats();

  public RMContainerAllocator(ClientService clientService, AppContext context) {
    super(clientService, context);
    this.stopped = new AtomicBoolean(false);
    this.clock = context.getClock();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    reduceSlowStart = conf.getFloat(
        MRJobConfig.COMPLETED_MAPS_FOR_REDUCE_SLOWSTART, 
        DEFAULT_COMPLETED_MAPS_PERCENT_FOR_REDUCE_SLOWSTART);
    maxReduceRampupLimit = conf.getFloat(
        MRJobConfig.MR_AM_JOB_REDUCE_RAMPUP_UP_LIMIT, 
        MRJobConfig.DEFAULT_MR_AM_JOB_REDUCE_RAMP_UP_LIMIT);
    maxReducePreemptionLimit = conf.getFloat(
        MRJobConfig.MR_AM_JOB_REDUCE_PREEMPTION_LIMIT,
        MRJobConfig.DEFAULT_MR_AM_JOB_REDUCE_PREEMPTION_LIMIT);
    allocationDelayThresholdMs = conf.getInt(
        MRJobConfig.MR_JOB_REDUCER_PREEMPT_DELAY_SEC,
        MRJobConfig.DEFAULT_MR_JOB_REDUCER_PREEMPT_DELAY_SEC) * 1000;//sec -> ms
    maxRunningMaps = conf.getInt(MRJobConfig.JOB_RUNNING_MAP_LIMIT,
        MRJobConfig.DEFAULT_JOB_RUNNING_MAP_LIMIT);
    maxRunningReduces = conf.getInt(MRJobConfig.JOB_RUNNING_REDUCE_LIMIT,
        MRJobConfig.DEFAULT_JOB_RUNNING_REDUCE_LIMIT);
    RackResolver.init(conf);
    retryInterval = getConfig().getLong(MRJobConfig.MR_AM_TO_RM_WAIT_INTERVAL_MS,
                                MRJobConfig.DEFAULT_MR_AM_TO_RM_WAIT_INTERVAL_MS);
    scheduledRequests.setConfig(getConfig());
    scheduledRequests.initAssignmentSolver();
    // Init startTime to current time. If all goes well, it will be reset after
    // first attempt to contact RM.
    retrystartTime = System.currentTimeMillis();
  }

  @Override
  protected void serviceStart() throws Exception {
    this.eventHandlingThread = new Thread() {
      @SuppressWarnings("unchecked")
      @Override
      public void run() {

        ContainerAllocatorEvent event;

        while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
          try {
            event = RMContainerAllocator.this.eventQueue.take();
          } catch (InterruptedException e) {
            if (!stopped.get()) {
              LOG.error("Returning, interrupted : " + e);
            }
            return;
          }

          try {
            handleEvent(event);
          } catch (Throwable t) {
            LOG.error("Error in handling event type " + event.getType()
                + " to the ContainreAllocator", t);
            // Kill the AM
            eventHandler.handle(new JobEvent(getJob().getID(),
              JobEventType.INTERNAL_ERROR));
            return;
          }
        }
      }
    };
    this.eventHandlingThread.start();
    super.serviceStart();
  }

  @Override
  protected synchronized void heartbeat() throws Exception {
    scheduleStats.updateAndLogIfChanged("Before Scheduling: ");
    List<Container> allocatedContainers = getResources();
    if (allocatedContainers != null && allocatedContainers.size() > 0) {
      scheduledRequests.assign(allocatedContainers);
    }

    int completedMaps = getJob().getCompletedMaps();
    int completedTasks = completedMaps + getJob().getCompletedReduces();
    if ((lastCompletedTasks != completedTasks) ||
          (scheduledRequests.maps.size() > 0)) {
      lastCompletedTasks = completedTasks;
      recalculateReduceSchedule = true;
    }

    if (recalculateReduceSchedule) {
      preemptReducesIfNeeded();
      scheduleReduces(
          getJob().getTotalMaps(), completedMaps,
          scheduledRequests.maps.size(), scheduledRequests.reduces.size(), 
          assignedRequests.maps.size(), assignedRequests.reduces.size(),
          mapResourceRequest, reduceResourceRequest,
          pendingReduces.size(), 
          maxReduceRampupLimit, reduceSlowStart);
      recalculateReduceSchedule = false;
    }

    scheduleStats.updateAndLogIfChanged("After Scheduling: ");
  }

  @Override
  protected void serviceStop() throws Exception {
    if (stopped.getAndSet(true)) {
      // return if already stopped
      return;
    }
    if (eventHandlingThread != null) {
      eventHandlingThread.interrupt();
    }
    super.serviceStop();
    scheduleStats.log("Final Stats: ");
  }

  @Private
  @VisibleForTesting
  AssignedRequests getAssignedRequests() {
    return assignedRequests;
  }

  @Private
  @VisibleForTesting
  ScheduledRequests getScheduledRequests() {
    return scheduledRequests;
  }

  public boolean getIsReduceStarted() {
    return reduceStarted;
  }
  
  public void setIsReduceStarted(boolean reduceStarted) {
    this.reduceStarted = reduceStarted; 
  }

  @Override
  public void handle(ContainerAllocatorEvent event) {
    int qSize = eventQueue.size();
    if (qSize != 0 && qSize % 1000 == 0) {
      LOG.info("Size of event-queue in RMContainerAllocator is " + qSize);
    }
    int remCapacity = eventQueue.remainingCapacity();
    if (remCapacity < 1000) {
      LOG.warn("Very low remaining capacity in the event-queue "
          + "of RMContainerAllocator: " + remCapacity);
    }
    try {
      eventQueue.put(event);
    } catch (InterruptedException e) {
      throw new YarnRuntimeException(e);
    }
  }

  @SuppressWarnings({ "unchecked" })
  protected synchronized void handleEvent(ContainerAllocatorEvent event) {
    recalculateReduceSchedule = true;
    if (event.getType() == ContainerAllocator.EventType.CONTAINER_REQ) {
      ContainerRequestEvent reqEvent = (ContainerRequestEvent) event;
      JobId jobId = getJob().getID();
      Resource supportedMaxContainerCapability = getMaxContainerCapability();
      if (reqEvent.getAttemptID().getTaskId().getTaskType().equals(TaskType.MAP)) {
        if (mapResourceRequest.equals(Resources.none())) {
          mapResourceRequest = reqEvent.getCapability();
          eventHandler.handle(new JobHistoryEvent(jobId,
            new NormalizedResourceEvent(
              org.apache.hadoop.mapreduce.TaskType.MAP, mapResourceRequest
                .getMemory())));
          LOG.info("mapResourceRequest:" + mapResourceRequest);
          if (mapResourceRequest.getMemory() > supportedMaxContainerCapability
            .getMemory()
              || mapResourceRequest.getVirtualCores() > supportedMaxContainerCapability
                .getVirtualCores()) {
            String diagMsg =
                "MAP capability required is more than the supported "
                    + "max container capability in the cluster. Killing the Job. mapResourceRequest: "
                    + mapResourceRequest + " maxContainerCapability:"
                    + supportedMaxContainerCapability;
            LOG.info(diagMsg);
            eventHandler.handle(new JobDiagnosticsUpdateEvent(jobId, diagMsg));
            eventHandler.handle(new JobEvent(jobId, JobEventType.JOB_KILL));
          }
        }
        // set the resources
        reqEvent.getCapability().setMemory(mapResourceRequest.getMemory());
        reqEvent.getCapability().setVirtualCores(
          mapResourceRequest.getVirtualCores());
        scheduledRequests.addMap(reqEvent);//maps are immediately scheduled
      } else {
        if (reduceResourceRequest.equals(Resources.none())) {
          reduceResourceRequest = reqEvent.getCapability();
          eventHandler.handle(new JobHistoryEvent(jobId,
            new NormalizedResourceEvent(
              org.apache.hadoop.mapreduce.TaskType.REDUCE,
              reduceResourceRequest.getMemory())));
          LOG.info("reduceResourceRequest:" + reduceResourceRequest);
          if (reduceResourceRequest.getMemory() > supportedMaxContainerCapability
            .getMemory()
              || reduceResourceRequest.getVirtualCores() > supportedMaxContainerCapability
                .getVirtualCores()) {
            String diagMsg =
                "REDUCE capability required is more than the "
                    + "supported max container capability in the cluster. Killing the "
                    + "Job. reduceResourceRequest: " + reduceResourceRequest
                    + " maxContainerCapability:"
                    + supportedMaxContainerCapability;
            LOG.info(diagMsg);
            eventHandler.handle(new JobDiagnosticsUpdateEvent(jobId, diagMsg));
            eventHandler.handle(new JobEvent(jobId, JobEventType.JOB_KILL));
          }
        }
        // set the resources
        reqEvent.getCapability().setMemory(reduceResourceRequest.getMemory());
        reqEvent.getCapability().setVirtualCores(
          reduceResourceRequest.getVirtualCores());
        if (reqEvent.getEarlierAttemptFailed()) {
          //add to the front of queue for fail fast
          pendingReduces.addFirst(new ContainerRequest(reqEvent, PRIORITY_REDUCE));
        } else {
          pendingReduces.add(new ContainerRequest(reqEvent, PRIORITY_REDUCE));
          //reduces are added to pending and are slowly ramped up
        }
      }
      
    } else if (
        event.getType() == ContainerAllocator.EventType.CONTAINER_DEALLOCATE) {
  
      LOG.info("Processing the event " + event.toString());

      TaskAttemptId aId = event.getAttemptID();
      
      boolean removed = scheduledRequests.remove(aId);
      if (!removed) {
        ContainerId containerId = assignedRequests.get(aId);
        if (containerId != null) {
          removed = true;
          assignedRequests.remove(aId);
          containersReleased++;
          pendingRelease.add(containerId);
          release(containerId);
        }
      }
      if (!removed) {
        LOG.error("Could not deallocate container for task attemptId " + 
            aId);
      }
    } else if (
        event.getType() == ContainerAllocator.EventType.CONTAINER_FAILED) {
      ContainerFailedEvent fEv = (ContainerFailedEvent) event;
      String host = getHost(fEv.getContMgrAddress());
      containerFailedOnHost(host);
    }
  }

  private static String getHost(String contMgrAddress) {
    String host = contMgrAddress;
    String[] hostport = host.split(":");
    if (hostport.length == 2) {
      host = hostport[0];
    }
    return host;
  }

  @Private
  @VisibleForTesting
  synchronized void setReduceResourceRequest(Resource res) {
    this.reduceResourceRequest = res;
  }

  @Private
  @VisibleForTesting
  synchronized void setMapResourceRequest(Resource res) {
    this.mapResourceRequest = res;
  }

  @Private
  @VisibleForTesting
  void preemptReducesIfNeeded() {
    if (reduceResourceRequest.equals(Resources.none())) {
      return; // no reduces
    }
    //check if reduces have taken over the whole cluster and there are 
    //unassigned maps
    if (scheduledRequests.maps.size() > 0) {
      Resource resourceLimit = getResourceLimit();
      Resource availableResourceForMap =
          Resources.subtract(
            resourceLimit,
            Resources.multiply(reduceResourceRequest,
              assignedRequests.reduces.size()
                  - assignedRequests.preemptionWaitingReduces.size()));
      // availableMemForMap must be sufficient to run at least 1 map
      if (ResourceCalculatorUtils.computeAvailableContainers(availableResourceForMap,
        mapResourceRequest, getSchedulerResourceTypes()) <= 0) {
        // to make sure new containers are given to maps and not reduces
        // ramp down all scheduled reduces if any
        // (since reduces are scheduled at higher priority than maps)
        LOG.info("Ramping down all scheduled reduces:"
            + scheduledRequests.reduces.size());
        for (ContainerRequest req : scheduledRequests.reduces.values()) {
          pendingReduces.add(req);
        }
        scheduledRequests.reduces.clear();
 
        //do further checking to find the number of map requests that were
        //hanging around for a while
        int hangingMapRequests = getNumOfHangingRequests(scheduledRequests.maps);
        if (hangingMapRequests > 0) {
          // preempt for making space for at least one map
          int preemptionReduceNumForOneMap =
              ResourceCalculatorUtils.divideAndCeilContainers(mapResourceRequest,
                reduceResourceRequest, getSchedulerResourceTypes());
          int preemptionReduceNumForPreemptionLimit =
              ResourceCalculatorUtils.divideAndCeilContainers(
                Resources.multiply(resourceLimit, maxReducePreemptionLimit),
                reduceResourceRequest, getSchedulerResourceTypes());
          int preemptionReduceNumForAllMaps =
              ResourceCalculatorUtils.divideAndCeilContainers(
                Resources.multiply(mapResourceRequest, hangingMapRequests),
                reduceResourceRequest, getSchedulerResourceTypes());
          int toPreempt =
              Math.min(Math.max(preemptionReduceNumForOneMap,
                preemptionReduceNumForPreemptionLimit),
                preemptionReduceNumForAllMaps);

          LOG.info("Going to preempt " + toPreempt
              + " due to lack of space for maps");
          assignedRequests.preemptReduce(toPreempt);
        }
      }
    }
  }
 
  private int getNumOfHangingRequests(Map<TaskAttemptId, ContainerRequest> requestMap) {
    if (allocationDelayThresholdMs <= 0)
      return requestMap.size();
    int hangingRequests = 0;
    long currTime = clock.getTime();
    for (ContainerRequest request: requestMap.values()) {
      long delay = currTime - request.requestTimeMs;
      if (delay > allocationDelayThresholdMs)
        hangingRequests++;
    }
    return hangingRequests;
  }
  
  @Private
  public void scheduleReduces(
      int totalMaps, int completedMaps,
      int scheduledMaps, int scheduledReduces,
      int assignedMaps, int assignedReduces,
      Resource mapResourceReqt, Resource reduceResourceReqt,
      int numPendingReduces,
      float maxReduceRampupLimit, float reduceSlowStart) {
    
    if (numPendingReduces == 0) {
      return;
    }
    
    // get available resources for this job
    Resource headRoom = getAvailableResources();
    if (headRoom == null) {
      headRoom = Resources.none();
    }

    LOG.info("Recalculating schedule, headroom=" + headRoom);
    
    //check for slow start
    if (!getIsReduceStarted()) {//not set yet
      int completedMapsForReduceSlowstart = (int)Math.ceil(reduceSlowStart * 
                      totalMaps);
      if(completedMaps < completedMapsForReduceSlowstart) {
        LOG.info("Reduce slow start threshold not met. " +
              "completedMapsForReduceSlowstart " + 
            completedMapsForReduceSlowstart);
        return;
      } else {
        LOG.info("Reduce slow start threshold reached. Scheduling reduces.");
        setIsReduceStarted(true);
      }
    }
    
    //if all maps are assigned, then ramp up all reduces irrespective of the
    //headroom
    if (scheduledMaps == 0 && numPendingReduces > 0) {
      LOG.info("All maps assigned. " +
          "Ramping up all remaining reduces:" + numPendingReduces);
      scheduleAllReduces();
      return;
    }

    float completedMapPercent = 0f;
    if (totalMaps != 0) {//support for 0 maps
      completedMapPercent = (float)completedMaps/totalMaps;
    } else {
      completedMapPercent = 1;
    }
    
    Resource netScheduledMapResource =
        Resources.multiply(mapResourceReqt, (scheduledMaps + assignedMaps));

    Resource netScheduledReduceResource =
        Resources.multiply(reduceResourceReqt,
          (scheduledReduces + assignedReduces));

    Resource finalMapResourceLimit;
    Resource finalReduceResourceLimit;

    // ramp up the reduces based on completed map percentage
    Resource totalResourceLimit = getResourceLimit();

    Resource idealReduceResourceLimit =
        Resources.multiply(totalResourceLimit,
          Math.min(completedMapPercent, maxReduceRampupLimit));
    Resource ideaMapResourceLimit =
        Resources.subtract(totalResourceLimit, idealReduceResourceLimit);

    // check if there aren't enough maps scheduled, give the free map capacity
    // to reduce.
    // Even when container number equals, there may be unused resources in one
    // dimension
    if (ResourceCalculatorUtils.computeAvailableContainers(ideaMapResourceLimit,
      mapResourceReqt, getSchedulerResourceTypes()) >= (scheduledMaps + assignedMaps)) {
      // enough resource given to maps, given the remaining to reduces
      Resource unusedMapResourceLimit =
          Resources.subtract(ideaMapResourceLimit, netScheduledMapResource);
      finalReduceResourceLimit =
          Resources.add(idealReduceResourceLimit, unusedMapResourceLimit);
      finalMapResourceLimit =
          Resources.subtract(totalResourceLimit, finalReduceResourceLimit);
    } else {
      finalMapResourceLimit = ideaMapResourceLimit;
      finalReduceResourceLimit = idealReduceResourceLimit;
    }

    LOG.info("completedMapPercent " + completedMapPercent
        + " totalResourceLimit:" + totalResourceLimit
        + " finalMapResourceLimit:" + finalMapResourceLimit
        + " finalReduceResourceLimit:" + finalReduceResourceLimit
        + " netScheduledMapResource:" + netScheduledMapResource
        + " netScheduledReduceResource:" + netScheduledReduceResource);

    int rampUp =
        ResourceCalculatorUtils.computeAvailableContainers(Resources.subtract(
                finalReduceResourceLimit, netScheduledReduceResource),
            reduceResourceReqt, getSchedulerResourceTypes());

    if (rampUp > 0) {
      rampUp = Math.min(rampUp, numPendingReduces);
      LOG.info("Ramping up " + rampUp);
      rampUpReduces(rampUp);
    } else if (rampUp < 0) {
      int rampDown = -1 * rampUp;
      rampDown = Math.min(rampDown, scheduledReduces);
      LOG.info("Ramping down " + rampDown);
      rampDownReduces(rampDown);
    }
  }

  @Private
  public void scheduleAllReduces() {
    for (ContainerRequest req : pendingReduces) {
      scheduledRequests.addReduce(req);
    }
    pendingReduces.clear();
  }
  
  @Private
  public void rampUpReduces(int rampUp) {
    //more reduce to be scheduled
    for (int i = 0; i < rampUp; i++) {
      ContainerRequest request = pendingReduces.removeFirst();
      scheduledRequests.addReduce(request);
    }
  }
  
  @Private
  public void rampDownReduces(int rampDown) {
    //remove from the scheduled and move back to pending
    for (int i = 0; i < rampDown; i++) {
      ContainerRequest request = scheduledRequests.removeReduce();
      pendingReduces.add(request);
    }
  }
  
  @SuppressWarnings("unchecked")
  private List<Container> getResources() throws Exception {
    applyConcurrentTaskLimits();

    // will be null the first time
    Resource headRoom =
        getAvailableResources() == null ? Resources.none() :
            Resources.clone(getAvailableResources());
    AllocateResponse response;
    /*
     * If contact with RM is lost, the AM will wait MR_AM_TO_RM_WAIT_INTERVAL_MS
     * milliseconds before aborting. During this interval, AM will still try
     * to contact the RM.
     */
    try {
      response = makeRemoteRequest();
      // Reset retry count if no exception occurred.
      retrystartTime = System.currentTimeMillis();
    } catch (ApplicationAttemptNotFoundException e ) {
      // This can happen if the RM has been restarted. If it is in that state,
      // this application must clean itself up.
      eventHandler.handle(new JobEvent(this.getJob().getID(),
        JobEventType.JOB_AM_REBOOT));
      throw new YarnRuntimeException(
        "Resource Manager doesn't recognize AttemptId: "
            + this.getContext().getApplicationAttemptId(), e);
    } catch (ApplicationMasterNotRegisteredException e) {
      LOG.info("ApplicationMaster is out of sync with ResourceManager,"
          + " hence resync and send outstanding requests.");
      // RM may have restarted, re-register with RM.
      lastResponseID = 0;
      register();
      addOutstandingRequestOnResync();
      return null;
    } catch (Exception e) {
      // This can happen when the connection to the RM has gone down. Keep
      // re-trying until the retryInterval has expired.
      if (System.currentTimeMillis() - retrystartTime >= retryInterval) {
        LOG.error("Could not contact RM after " + retryInterval + " milliseconds.");
        eventHandler.handle(new JobEvent(this.getJob().getID(),
                                         JobEventType.JOB_AM_REBOOT));
        throw new YarnRuntimeException("Could not contact RM after " +
                                retryInterval + " milliseconds.");
      }
      // Throw this up to the caller, which may decide to ignore it and
      // continue to attempt to contact the RM.
      throw e;
    }
    Resource newHeadRoom =
        getAvailableResources() == null ? Resources.none()
            : getAvailableResources();
    List<Container> newContainers = response.getAllocatedContainers();
    // Setting NMTokens
    if (response.getNMTokens() != null) {
      for (NMToken nmToken : response.getNMTokens()) {
        NMTokenCache.setNMToken(nmToken.getNodeId().toString(),
            nmToken.getToken());
      }
    }

    // Setting AMRMToken
    if (response.getAMRMToken() != null) {
      updateAMRMToken(response.getAMRMToken());
    }

    List<ContainerStatus> finishedContainers = response.getCompletedContainersStatuses();
    if (newContainers.size() + finishedContainers.size() > 0
        || !headRoom.equals(newHeadRoom)) {
      //something changed
      recalculateReduceSchedule = true;
      if (LOG.isDebugEnabled() && !headRoom.equals(newHeadRoom)) {
        LOG.debug("headroom=" + newHeadRoom);
      }
    }

    if (LOG.isDebugEnabled()) {
      for (Container cont : newContainers) {
        LOG.debug("Received new Container :" + cont);
      }
    }

    //Called on each allocation. Will know about newly blacklisted/added hosts.
    computeIgnoreBlacklisting();

    handleUpdatedNodes(response);

    for (ContainerStatus cont : finishedContainers) {
      LOG.info("Received completed container " + cont.getContainerId());
      TaskAttemptId attemptID = assignedRequests.get(cont.getContainerId());
      if (attemptID == null) {
        LOG.error("Container complete event for unknown container id "
            + cont.getContainerId());
      } else {
        pendingRelease.remove(cont.getContainerId());
        assignedRequests.remove(attemptID);
        
        // send the container completed event to Task attempt
        eventHandler.handle(createContainerFinishedEvent(cont, attemptID));
        
        // Send the diagnostics
        String diagnostics = StringInterner.weakIntern(cont.getDiagnostics());
        eventHandler.handle(new TaskAttemptDiagnosticsUpdateEvent(attemptID,
            diagnostics));
      }      
    }
    return newContainers;
  }

  private void applyConcurrentTaskLimits() {
    int numScheduledMaps = scheduledRequests.maps.size();
    if (maxRunningMaps > 0 && numScheduledMaps > 0) {
      int maxRequestedMaps = Math.max(0,
          maxRunningMaps - assignedRequests.maps.size());
      int numScheduledFailMaps = scheduledRequests.earlierFailedMaps.size();
      int failedMapRequestLimit = Math.min(maxRequestedMaps,
          numScheduledFailMaps);
      int normalMapRequestLimit = Math.min(
          maxRequestedMaps - failedMapRequestLimit,
          numScheduledMaps - numScheduledFailMaps);
      setRequestLimit(PRIORITY_FAST_FAIL_MAP, mapResourceRequest,
          failedMapRequestLimit);
      setRequestLimit(PRIORITY_MAP, mapResourceRequest, normalMapRequestLimit);
    }

    int numScheduledReduces = scheduledRequests.reduces.size();
    if (maxRunningReduces > 0 && numScheduledReduces > 0) {
      int maxRequestedReduces = Math.max(0,
          maxRunningReduces - assignedRequests.reduces.size());
      int reduceRequestLimit = Math.min(maxRequestedReduces,
          numScheduledReduces);
      setRequestLimit(PRIORITY_REDUCE, reduceResourceRequest,
          reduceRequestLimit);
    }
  }

  private boolean canAssignMaps() {
    return (maxRunningMaps <= 0
        || assignedRequests.maps.size() < maxRunningMaps);
  }

  private boolean canAssignReduces() {
    return (maxRunningReduces <= 0
        || assignedRequests.reduces.size() < maxRunningReduces);
  }

  private void updateAMRMToken(Token token) throws IOException {
    org.apache.hadoop.security.token.Token<AMRMTokenIdentifier> amrmToken =
        new org.apache.hadoop.security.token.Token<AMRMTokenIdentifier>(token
          .getIdentifier().array(), token.getPassword().array(), new Text(
          token.getKind()), new Text(token.getService()));
    UserGroupInformation currentUGI = UserGroupInformation.getCurrentUser();
    currentUGI.addToken(amrmToken);
    amrmToken.setService(ClientRMProxy.getAMRMTokenService(getConfig()));
  }

  @VisibleForTesting
  public TaskAttemptEvent createContainerFinishedEvent(ContainerStatus cont,
      TaskAttemptId attemptID) {
    if (cont.getExitStatus() == ContainerExitStatus.ABORTED
        || cont.getExitStatus() == ContainerExitStatus.PREEMPTED) {
      // killed by framework
      return new TaskAttemptEvent(attemptID,
          TaskAttemptEventType.TA_KILL);
    } else {
      return new TaskAttemptEvent(attemptID,
          TaskAttemptEventType.TA_CONTAINER_COMPLETED);
    }
  }
  
  @SuppressWarnings("unchecked")
  private void handleUpdatedNodes(AllocateResponse response) {
    // send event to the job about on updated nodes
    List<NodeReport> updatedNodes = response.getUpdatedNodes();
    if (!updatedNodes.isEmpty()) {

      // send event to the job to act upon completed tasks
      eventHandler.handle(new JobUpdatedNodesEvent(getJob().getID(),
          updatedNodes));

      // act upon running tasks
      HashSet<NodeId> unusableNodes = new HashSet<NodeId>();
      for (NodeReport nr : updatedNodes) {
        NodeState nodeState = nr.getNodeState();
        if (nodeState.isUnusable()) {
          unusableNodes.add(nr.getNodeId());
        }
      }
      for (int i = 0; i < 2; ++i) {
        HashMap<TaskAttemptId, Container> taskSet = i == 0 ? assignedRequests.maps
            : assignedRequests.reduces;
        // kill running containers
        for (Map.Entry<TaskAttemptId, Container> entry : taskSet.entrySet()) {
          TaskAttemptId tid = entry.getKey();
          NodeId taskAttemptNodeId = entry.getValue().getNodeId();
          if (unusableNodes.contains(taskAttemptNodeId)) {
            LOG.info("Killing taskAttempt:" + tid
                + " because it is running on unusable node:"
                + taskAttemptNodeId);
            eventHandler.handle(new TaskAttemptKillEvent(tid,
                "TaskAttempt killed because it ran on unusable node"
                    + taskAttemptNodeId));
          }
        }
      }
    }
  }

  @Private
  public Resource getResourceLimit() {
    Resource headRoom = getAvailableResources();
    if (headRoom == null) {
      headRoom = Resources.none();
    }
    Resource assignedMapResource =
        Resources.multiply(mapResourceRequest, assignedRequests.maps.size());
    Resource assignedReduceResource =
        Resources.multiply(reduceResourceRequest,
          assignedRequests.reduces.size());
    return Resources.add(headRoom,
      Resources.add(assignedMapResource, assignedReduceResource));
  }

  @Private
  @VisibleForTesting
  class ScheduledRequests {
    
    private Configuration conf = null;
     
    private final LinkedList<TaskAttemptId> earlierFailedMaps = 
      new LinkedList<TaskAttemptId>();

    // pendingCache = tasks with input waiting to be cached
    private final Map<TaskAttemptId, TaskSplitMetaInfo> pendingCache = new HashMap<>();
    private final HashMap<Path, long[]> pathsToCache = new HashMap<>();
    
    @VisibleForTesting
    final Map<TaskAttemptId, ContainerRequest> maps =
      new LinkedHashMap<TaskAttemptId, ContainerRequest>();
    
    private final LinkedHashMap<TaskAttemptId, ContainerRequest> reduces = 
      new LinkedHashMap<TaskAttemptId, ContainerRequest>();
    
    private AMAssignmentSolver solver = new AMAssignmentSolverDefault();

    public void initAssignmentSolver() {
        Class<?> solverClass = getConfig().getClass(
                MRJobConfig.MR_AM_TASK_ASSIGNMENT_SOLVER_CLASS, 
                AMAssignmentSolverDefault.class);
        // Note: Cannot use Java reflection because these classes are non-static inner
        if (solverClass.equals(AMAssignmentSolverDefault.class))
            solver = new AMAssignmentSolverDefault();
        else if (solverClass.equals(AMAssignmentSolverHungarian.class))
            solver = new AMAssignmentSolverHungarian();
        else
            throw new RuntimeException("Unknown AMAssignmentSolver class " + solverClass);
        
        solver.enableTiers(getConfig().getBoolean(
                MRJobConfig.MR_AM_TASK_ASSIGNMENT_ENABLE_TIERS, false));
    }

    boolean remove(TaskAttemptId tId) {
      ContainerRequest req = null;
      if (tId.getTaskId().getTaskType().equals(TaskType.MAP)) {
        req = maps.remove(tId);
      } else {
        req = reduces.remove(tId);
      }
      
      if (req == null) {
        return false;
      } else {
        decContainerReq(req);
        return true;
      }
    }
    
    void setConfig(Configuration config) {
      this.conf = config;
   }
    
    Configuration getConfig() {
       if (conf == null)
          conf = new Configuration();

       return conf;
    }

   ContainerRequest removeReduce() {
      Iterator<Entry<TaskAttemptId, ContainerRequest>> it = reduces.entrySet().iterator();
      if (it.hasNext()) {
        Entry<TaskAttemptId, ContainerRequest> entry = it.next();
        it.remove();
        decContainerReq(entry.getValue());
        return entry.getValue();
      }
      return null;
    }
    
    void addMap(ContainerRequestEvent event){
      ContainerRequest request;
      
      if (event.getEarlierAttemptFailed()) {
        earlierFailedMaps.add(event.getAttemptID());
        request = new ContainerRequest(event, PRIORITY_FAST_FAIL_MAP);
        LOG.info("Added "+event.getAttemptID()+" to list of failed maps");
      } else {
          // Create new map container request
          request = new ContainerRequest(event, PRIORITY_MAP);
          solver.addContainerRequest(event.getAttemptID(), request);
      }
      
      // Handle pending cache requests
      if (solver.isTierAware() && MRPrefetcher.isPrefetchingEnabled(getConfig())) {
         TaskSplitMetaInfo info = event.getSplitMetaInfo();
         if (info != null && info.getFilePath() != null && info.getLocations() != null
               && info.getLocations().length > 0 && !info.hasAnyCachedOrMemory()
               && info.getInputDataStart() >= 0) {
            // Look for cache locations and update container request
            if (updateContainerRequestCacheLocations(request, event.getAttemptID(), info)) {
               // Unable to find cache locations - save it for later
               pendingCache.put(event.getAttemptID(), info);
               LOG.info("Added " + event.getAttemptID() + " to list of pending cache");
               
               if (pathsToCache.containsKey(info.getFilePath())) {
                  long[] data = pathsToCache.get(info.getFilePath());
                  data[0] += 1;
                  data[1] += info.getInputDataLength();
               } else {
                  long[] data = new long[2];
                  data[0] = 1;
                  data[1] = info.getInputDataLength();
                  pathsToCache.put(info.getFilePath(), data);
               }
            }
         }
      }
      
      maps.put(event.getAttemptID(), request);
      addContainerReq(request);
    }
        
    void addReduce(ContainerRequest req) {
      reduces.put(req.attemptID, req);
      addContainerReq(req);
    }
    
    // this method will change the list of allocatedContainers.
    private void assign(List<Container> allocatedContainers) {
      Iterator<Container> it = allocatedContainers.iterator();
      LOG.info("Got allocated containers " + allocatedContainers.size());
      containersAllocated += allocatedContainers.size();
      while (it.hasNext()) {
        Container allocated = it.next();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Assigning container " + allocated.getId()
              + " with priority " + allocated.getPriority() + " to NM "
              + allocated.getNodeId());
        }
        
        // check if allocated container meets memory requirements 
        // and whether we have any scheduled tasks that need 
        // a container to be assigned
        boolean isAssignable = true;
        Priority priority = allocated.getPriority();
        Resource allocatedResource = allocated.getResource();
        if (PRIORITY_FAST_FAIL_MAP.equals(priority) 
            || PRIORITY_MAP.equals(priority)) {
          if (ResourceCalculatorUtils.computeAvailableContainers(allocatedResource,
              mapResourceRequest, getSchedulerResourceTypes()) <= 0
              || maps.isEmpty()) {
            LOG.info("Cannot assign container " + allocated 
                + " for a map as either "
                + " container memory less than required " + mapResourceRequest
                + " or no pending map tasks - maps.isEmpty=" 
                + maps.isEmpty()); 
            isAssignable = false; 
          }
        } 
        else if (PRIORITY_REDUCE.equals(priority)) {
          if (ResourceCalculatorUtils.computeAvailableContainers(allocatedResource,
              reduceResourceRequest, getSchedulerResourceTypes()) <= 0
              || reduces.isEmpty()) {
            LOG.info("Cannot assign container " + allocated 
                + " for a reduce as either "
                + " container memory less than required " + reduceResourceRequest
                + " or no pending reduce tasks - reduces.isEmpty=" 
                + reduces.isEmpty()); 
            isAssignable = false;
          }
        } else {
          LOG.warn("Container allocated at unwanted priority: " + priority + 
              ". Returning to RM...");
          isAssignable = false;
        }
        
        if(!isAssignable) {
          // release container if we could not assign it 
          containerNotAssigned(allocated);
          it.remove();
          continue;
        }
        
        // do not assign if allocated container is on a  
        // blacklisted host
        String allocatedHost = allocated.getNodeId().getHost();
        if (isNodeBlacklisted(allocatedHost)) {
          // we need to request for a new container 
          // and release the current one
          LOG.info("Got allocated container on a blacklisted "
              + " host "+allocatedHost
              +". Releasing container " + allocated);

          // find the request matching this allocated container 
          // and replace it with a new one 
          ContainerRequest toBeReplacedReq = 
              getContainerReqToReplace(allocated);
          if (toBeReplacedReq != null) {
            LOG.info("Placing a new container request for task attempt " 
                + toBeReplacedReq.attemptID);
            ContainerRequest newReq = 
                getFilteredContainerRequest(toBeReplacedReq);
            decContainerReq(toBeReplacedReq);
            if (toBeReplacedReq.attemptID.getTaskId().getTaskType() ==
                TaskType.MAP) {
              maps.put(newReq.attemptID, newReq);
            }
            else {
              reduces.put(newReq.attemptID, newReq);
            }
            addContainerReq(newReq);
          }
          else {
            LOG.info("Could not map allocated container to a valid request."
                + " Releasing allocated container " + allocated);
          }
          
          // release container if we could not assign it 
          containerNotAssigned(allocated);
          it.remove();
          continue;
        }
      }

      assignContainers(allocatedContainers);
       
      // release container if we could not assign it 
      it = allocatedContainers.iterator();
      while (it.hasNext()) {
        Container allocated = it.next();
        LOG.info("Releasing unassigned container " + allocated);
        containerNotAssigned(allocated);
      }
    }
    
    @SuppressWarnings("unchecked")
    void containerAssigned(Container allocated, 
                                    ContainerRequest assigned) {
      // Update resource requests
      decContainerReq(assigned);

      // send the container-assigned event to task attempt
      eventHandler.handle(new TaskAttemptContainerAssignedEvent(
          assigned.attemptID, allocated, applicationACLs));

      assignedRequests.add(allocated, assigned.attemptID);

      if (LOG.isDebugEnabled()) {
        LOG.info("Assigned container (" + allocated + ") "
            + " to task " + assigned.attemptID + " on node "
            + allocated.getNodeId().toString());
      }
    }
    
    private void containerNotAssigned(Container allocated) {
      containersReleased++;
      pendingRelease.add(allocated.getId());
      release(allocated.getId());      
    }
    
    private ContainerRequest assignWithoutLocality(Container allocated) {
      ContainerRequest assigned = null;
      
      Priority priority = allocated.getPriority();
      if (PRIORITY_FAST_FAIL_MAP.equals(priority)) {
        LOG.info("Assigning container " + allocated + " to fast fail map");
        assigned = assignToFailedMap(allocated);
      } else if (PRIORITY_REDUCE.equals(priority)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Assigning container " + allocated + " to reduce");
        }
        assigned = assignToReduce(allocated);
      }
        
      return assigned;
    }
        
    private void assignContainers(List<Container> allocatedContainers) {
      Iterator<Container> it = allocatedContainers.iterator();
      while (it.hasNext()) {
        Container allocated = it.next();
        ContainerRequest assigned = assignWithoutLocality(allocated);
        if (assigned != null) {
          containerAssigned(allocated, assigned);
          it.remove();
        }
      }

      int max = getConfig().getInt(
            MRJobConfig.MR_AM_MAX_CONTAINERS_TO_ASSIGN, Integer.MAX_VALUE);
      if (allocatedContainers.size() > max) {
         // Do not assign map tasks to all containers in this round
         Collections.shuffle(allocatedContainers);
         it = allocatedContainers.iterator();
         int count = 0;
         while (it.hasNext()) {
            ++count;
            Container allocated = it.next();
            if (count > max) {
               containerNotAssigned(allocated);
               it.remove();
            }
         }
         if (count > max)
            LOG.info("Released containers " + (count - max));
      }

      assignMapsWithLocality(allocatedContainers);
      
      for(ContainerRequest request : maps.values()) {
         decContainerReq(request);
         addContainerReq(request);
      }
    }
    
    private ContainerRequest getContainerReqToReplace(Container allocated) {
      LOG.info("Finding containerReq for allocated container: " + allocated);
      Priority priority = allocated.getPriority();
      ContainerRequest toBeReplaced = null;
      if (PRIORITY_FAST_FAIL_MAP.equals(priority)) {
        LOG.info("Replacing FAST_FAIL_MAP container " + allocated.getId());
        Iterator<TaskAttemptId> iter = earlierFailedMaps.iterator();
        while (toBeReplaced == null && iter.hasNext()) {
          toBeReplaced = maps.get(iter.next());
        }
        LOG.info("Found replacement: " + toBeReplaced);
        return toBeReplaced;
      }
      else if (PRIORITY_MAP.equals(priority)) {
        LOG.info("Replacing MAP container " + allocated.getId());
        // allocated container was for a map
        TaskAttemptId tId = solver.getReplacementTaskAttemptId(allocated, this);
        if (tId != null && maps.containsKey(tId)) {
            toBeReplaced = maps.remove(tId);
        } else {
          tId = maps.keySet().iterator().next();
          toBeReplaced = maps.remove(tId);          
        }        
      }
      else if (PRIORITY_REDUCE.equals(priority)) {
        TaskAttemptId tId = reduces.keySet().iterator().next();
        toBeReplaced = reduces.remove(tId);    
      }
      LOG.info("Found replacement: " + toBeReplaced);
      return toBeReplaced;
    }
    
    
    @SuppressWarnings("unchecked")
    private ContainerRequest assignToFailedMap(Container allocated) {
      //try to assign to earlierFailedMaps if present
      ContainerRequest assigned = null;
      while (assigned == null && earlierFailedMaps.size() > 0
          && canAssignMaps()) {
        TaskAttemptId tId = earlierFailedMaps.removeFirst();      
        if (maps.containsKey(tId)) {
          assigned = maps.remove(tId);
          JobCounterUpdateEvent jce =
            new JobCounterUpdateEvent(assigned.attemptID.getTaskId().getJobId());
          jce.addCounterUpdate(JobCounter.OTHER_LOCAL_MAPS, 1);
          eventHandler.handle(jce);
          LOG.info("Assigned from earlierFailedMaps");
          break;
        }
      }
      return assigned;
    }
    
    private ContainerRequest assignToReduce(Container allocated) {
      ContainerRequest assigned = null;
      //try to assign to reduces if present
      if (assigned == null && reduces.size() > 0 && canAssignReduces()) {
        TaskAttemptId tId = reduces.keySet().iterator().next();
        assigned = reduces.remove(tId);
        LOG.info("Assigned to reduce");
      }
      return assigned;
    }

    private void assignMapsWithLocality(List<Container> allocatedContainers) {
      
      boolean enablePrefetching = MRPrefetcher.isPrefetchingEnabled(getConfig());
      if (enablePrefetching && !pendingCache.isEmpty() && !maps.isEmpty()) {
         // Refresh pending cache
         HashMap<TaskAttemptId, TaskSplitMetaInfo> toBeDeleted = new HashMap<>();
         for (Entry<TaskAttemptId, TaskSplitMetaInfo> entry : pendingCache.entrySet()) {
            TaskAttemptId id = entry.getKey();
            TaskSplitMetaInfo info = entry.getValue();
            if (maps.containsKey(id)) {
               // Look for cache locations and update container request
               if (!updateContainerRequestCacheLocations(maps.get(id), id, info)) {
                  // Either found cache locations or not able to find anything
                  toBeDeleted.put(id, info);
               }
            } else {
               // task attempt has already been scheduled
               LOG.info("Task has already been scheduled - don't look for cache for " + id);
               toBeDeleted.put(id, info);
            }
         }
         
         // Delete task attempts from both pending cache and paths to cache
         for (Entry<TaskAttemptId, TaskSplitMetaInfo> entry : toBeDeleted.entrySet()) {
            pendingCache.remove(entry.getKey());
            pathsToCache.remove(entry.getValue().getFilePath());
         }
      }
      
      // Make the assignment of map task attempts to containers
      solver.assignContainers(allocatedContainers, this);
    }
    
    /**
     * Update the container request with cache locations of the input split
     * 
     * @param request
     * @param id
     * @param info
     * @return true if finding cache locations is still pending.
     *         false if found cache locations or not able to find anything.
     */
    private boolean updateContainerRequestCacheLocations(ContainerRequest request, 
          TaskAttemptId id, TaskSplitMetaInfo info) {
       boolean stillPending = true;
       try {
          // Get block locations for the file and append the cache hosts if found
          Path path = info.getFilePath();
          FileSystem fs = path.getFileSystem(getConfig());
          BlockLocation[] blks = fs.getFileBlockLocations(path, 
                info.getInputDataStart(), info.getInputDataLength());
          
          if (blks != null) {
            for (BlockLocation blkLocation : blks) {
               String[] cachedHosts = blkLocation.getCachedHosts();
               String[] pendingCachedHosts = blkLocation.getPendingCachedHosts();
               if (cachedHosts != null && cachedHosts.length > 0) {
                  // Update the cached hosts
                  for (String cachedHost: cachedHosts) {
                      request.types.put(cachedHost, StorageType.MEMORY);
                      request.types.put(solver.resolveRack(cachedHost), 
                              StorageType.MEMORY);
                      LOG.info("Found new cache location (" + cachedHost 
                            + ") for task " + id);
                   }
                  
                  solver.updateContainerRequest(id, request);
                  stillPending = false;
               } else if (pendingCachedHosts != null && pendingCachedHosts.length > 0) {
                  // Update the pending cached hosts
                  String cachedRack;
                  for (String cachedHost: pendingCachedHosts) {
                      if (!request.types.containsKey(cachedHost)
                            || request.types.get(cachedHost) != StorageType.MEMORY)
                          request.types.put(cachedHost, StorageType.RAM_DISK);
                      cachedRack = solver.resolveRack(cachedHost);
                      if (!request.types.containsKey(cachedRack)
                            || request.types.get(cachedRack) != StorageType.MEMORY)
                         request.types.put(cachedRack, StorageType.RAM_DISK);
                      LOG.info("Found new pending cache location (" + cachedHost
                            + ") for task " + id);
                  }
                  
                  solver.updateContainerRequest(id, request);
                  stillPending = true;
               }

            }
          } else {
             // unable to find block locations
             LOG.info("No block locations found for task " + id);
             stillPending = false;
          }

       } catch (IOException e) {
          LOG.warn("Error when looking for block locations for task " + id + ": "
                + e.getClass().getSimpleName() + ": " + e.getLocalizedMessage().split("\n")[0]);
          stillPending = false;
       }

       return stillPending;
    }
  }

  @Private
  @VisibleForTesting
  class AssignedRequests {
    private final Map<ContainerId, TaskAttemptId> containerToAttemptMap =
      new HashMap<ContainerId, TaskAttemptId>();
    private final LinkedHashMap<TaskAttemptId, Container> maps = 
      new LinkedHashMap<TaskAttemptId, Container>();
    @VisibleForTesting
    final LinkedHashMap<TaskAttemptId, Container> reduces =
      new LinkedHashMap<TaskAttemptId, Container>();
    @VisibleForTesting
    final Set<TaskAttemptId> preemptionWaitingReduces =
      new HashSet<TaskAttemptId>();
    
    void add(Container container, TaskAttemptId tId) {
      LOG.info("Assigned container " + container.getId().toString() + " to " + tId);
      containerToAttemptMap.put(container.getId(), tId);
      if (tId.getTaskId().getTaskType().equals(TaskType.MAP)) {
        maps.put(tId, container);
      } else {
        reduces.put(tId, container);
      }
    }

    @SuppressWarnings("unchecked")
    void preemptReduce(int toPreempt) {
      List<TaskAttemptId> reduceList = new ArrayList<TaskAttemptId>
        (reduces.keySet());
      //sort reduces on progress
      Collections.sort(reduceList,
          new Comparator<TaskAttemptId>() {
        @Override
        public int compare(TaskAttemptId o1, TaskAttemptId o2) {
          return Float.compare(
              getJob().getTask(o1.getTaskId()).getAttempt(o1).getProgress(),
              getJob().getTask(o2.getTaskId()).getAttempt(o2).getProgress());
        }
      });
      
      for (int i = 0; i < toPreempt && reduceList.size() > 0; i++) {
        TaskAttemptId id = reduceList.remove(0);//remove the one on top
        LOG.info("Preempting " + id);
        preemptionWaitingReduces.add(id);
        eventHandler.handle(new TaskAttemptKillEvent(id, RAMPDOWN_DIAGNOSTIC));
      }
    }
    
    boolean remove(TaskAttemptId tId) {
      ContainerId containerId = null;
      if (tId.getTaskId().getTaskType().equals(TaskType.MAP)) {
        containerId = maps.remove(tId).getId();
      } else {
        containerId = reduces.remove(tId).getId();
        if (containerId != null) {
          boolean preempted = preemptionWaitingReduces.remove(tId);
          if (preempted) {
            LOG.info("Reduce preemption successful " + tId);
          }
        }
      }
      
      if (containerId != null) {
        containerToAttemptMap.remove(containerId);
        return true;
      }
      return false;
    }
    
    TaskAttemptId get(ContainerId cId) {
      return containerToAttemptMap.get(cId);
    }

    ContainerId get(TaskAttemptId tId) {
      Container taskContainer;
      if (tId.getTaskId().getTaskType().equals(TaskType.MAP)) {
        taskContainer = maps.get(tId);
      } else {
        taskContainer = reduces.get(tId);
      }

      if (taskContainer == null) {
        return null;
      } else {
        return taskContainer.getId();
      }
    }
  }

  private class ScheduleStats {
    int numPendingReduces;
    int numScheduledMaps;
    int numScheduledReduces;
    int numAssignedMaps;
    int numAssignedReduces;
    int numCompletedMaps;
    int numCompletedReduces;
    int numContainersAllocated;
    int numContainersReleased;

    public void updateAndLogIfChanged(String msgPrefix) {
      boolean changed = false;

      // synchronized to fix findbug warnings
      synchronized (RMContainerAllocator.this) {
        changed |= (numPendingReduces != pendingReduces.size());
        numPendingReduces = pendingReduces.size();
        changed |= (numScheduledMaps != scheduledRequests.maps.size());
        numScheduledMaps = scheduledRequests.maps.size();
        changed |= (numScheduledReduces != scheduledRequests.reduces.size());
        numScheduledReduces = scheduledRequests.reduces.size();
        changed |= (numAssignedMaps != assignedRequests.maps.size());
        numAssignedMaps = assignedRequests.maps.size();
        changed |= (numAssignedReduces != assignedRequests.reduces.size());
        numAssignedReduces = assignedRequests.reduces.size();
        changed |= (numCompletedMaps != getJob().getCompletedMaps());
        numCompletedMaps = getJob().getCompletedMaps();
        changed |= (numCompletedReduces != getJob().getCompletedReduces());
        numCompletedReduces = getJob().getCompletedReduces();
        changed |= (numContainersAllocated != containersAllocated);
        numContainersAllocated = containersAllocated;
        changed |= (numContainersReleased != containersReleased);
        numContainersReleased = containersReleased;
      }

      if (changed) {
        log(msgPrefix);
      }
    }

    public void log(String msgPrefix) {
        LOG.info(msgPrefix + "PendingReds:" + numPendingReduces +
        " ScheduledMaps:" + numScheduledMaps +
        " ScheduledReds:" + numScheduledReduces +
        " AssignedMaps:" + numAssignedMaps +
        " AssignedReds:" + numAssignedReduces +
        " CompletedMaps:" + numCompletedMaps +
        " CompletedReds:" + numCompletedReduces +
        " ContAlloc:" + numContainersAllocated +
        " ContRel:" + numContainersReleased +
        " HostLocal:" + hostLocalAssigned +
        " RackLocal:" + rackLocalAssigned);
    }
  }

  /**
   * An abstract base class for the Assignment Solver. A solver is responsible
   * for assigning map task attempts to allocated containers.
   * 
   * @author hero
   */
  abstract class AMAssignmentSolver {
      
      private boolean enableTiers = false;
      private HashMap<String, String> hostToRack = new HashMap<String, String>();

      /**
       * Set option to enable taking storage tiers into account
       * @param enableTiers
       */
      public void enableTiers(boolean enableTiers) {
          this.enableTiers = enableTiers;
      }
      
      /**
       * @return true if the solver takes storage tiers into account
       */
      public boolean isTierAware() {
          return enableTiers;
      }
      
      /**
       * Inform the solver that a new container request has been created.
       * 
       * @param id
       * @param req
       */
      abstract public void addContainerRequest(TaskAttemptId id, ContainerRequest req);
      
      /**
       * Inform the solver that a container request has been updated 
       * (host and storage types might have changed).
       * 
       * @param id
       * @param req
       */
      abstract public void updateContainerRequest(TaskAttemptId id, ContainerRequest req);
      
      /**
       * Find and return the task attempt id with the request that resulted to 
       * this allocated container
       *  
       * @param allocated
       * @param requests
       * @return
       */
      abstract public TaskAttemptId getReplacementTaskAttemptId(Container allocated,
              ScheduledRequests requests);
      
      /**
       * Perform the assignment of tasks to the provided containers. This method is
       * expected to use/modify the 'requests' parameter.
       * 
       * @param allocatedContainers
       * @param requests
       */
      abstract public void assignContainers(List<Container> allocatedContainers, ScheduledRequests requests);
      
      /**
       * Resolve the rack of the host
       * 
       * @param host
       * @return
       */
      protected String resolveRack(String host) {
          if (hostToRack.containsKey(host))
              return hostToRack.get(host);
          
          String rack = RackResolver.resolve(host).getNetworkLocation();
          hostToRack.put(host, rack);
          return rack;
      }
  }
  
  /**
   * The default assignment solver.
   * 
   * Approach: For each container, find a task that is either data local, 
   * rack local, or non local (in this order).
   * 
   * If storage tiers is enabled, then the tasks are traversed from the 
   * higher storage tier (e.g., memory) to the lower ones for each case
   * (data local, rack local, non local).
   * 
   * @author hero
   */
  class AMAssignmentSolverDefault extends AMAssignmentSolver {

      /**
       * Maps from a host/rack to a storage type and a list of Map tasks with data on
       * the host/rack
       */
      private final MapsLocationMappingStorageType mapsLocationMappingStorageType = new MapsLocationMappingStorageType();

      @Override
      public void addContainerRequest(TaskAttemptId id, ContainerRequest req) {
          // Create the mapping: host -> type -> attempt
          StorageType storageType = StorageType.DEFAULT;
          for (String host : req.hosts) {
              if (isTierAware())
                  storageType = req.types.get(host);
              
              mapsLocationMappingStorageType.setLocationTaskAttemptID(host, storageType,
                      id, MapsLocationMappingStorageType.Location.HOSTLOCAL);
              if (LOG.isDebugEnabled()) {
                  LOG.debug("Added attempt req " + id + " to host " + host + "/" + storageType);
              }
          }
          
          // Create the mapping: rack -> type -> attempt
          for (String rack : req.racks) {
              if (isTierAware())
                  storageType = req.types.get(rack);

              mapsLocationMappingStorageType.setLocationTaskAttemptID(rack, storageType,
                      id, MapsLocationMappingStorageType.Location.RACKLOCAL);
              if (LOG.isDebugEnabled()) {
                  LOG.debug("Added attempt req " + id + " to rack " + rack + "/" + storageType);
              }
          }
          
          LOG.info("Added attempt req " + id);
      }

      @Override
      public void updateContainerRequest(TaskAttemptId id, ContainerRequest req) {
          // Update the mapping: host -> type -> attempt
          for (String host : req.hosts) {
              mapsLocationMappingStorageType.setLocationTaskAttemptID(host, req.types.get(host),
                      id, MapsLocationMappingStorageType.Location.HOSTLOCAL);
              if (LOG.isDebugEnabled()) {
                  LOG.debug("Updated attempt req " + id + " to host " + host + "/" + req.types.get(host));
              }
          }

          // Update the mapping: rack -> type -> attempt
          for (String rack : req.racks) {
              mapsLocationMappingStorageType.setLocationTaskAttemptID(rack, req.types.get(rack),
                      id, MapsLocationMappingStorageType.Location.RACKLOCAL);
              if (LOG.isDebugEnabled()) {
                  LOG.debug("Updated attempt req " + id + " to rack " + rack + "/" + req.types.get(rack));
              }
          }

          LOG.info("Updated attempt req " + id);
      }

      @Override
      public TaskAttemptId getReplacementTaskAttemptId(Container allocated,
              ScheduledRequests requests) {
          // Return a random id from the list of tasks that match the host
          String host = allocated.getNodeId().getHost();
          LinkedList<TaskAttemptId> list = mapsLocationMappingStorageType.lastTaskAttemptIDList(host);
          if (list != null && list.size() > 0) {
              return list.get((int) (Math.random() * list.size()));
          }

          return null;
      }
      
      @SuppressWarnings("unchecked")
      public void assignContainers(List<Container> allocatedContainers,
              ScheduledRequests requests) {
          
          // try to assign to all nodes first to match node local
          Iterator<Container> it = allocatedContainers.iterator();
          while(it.hasNext() && requests.maps.size() > 0 && canAssignMaps()){
            Container allocated = it.next();        
            Priority priority = allocated.getPriority();
            assert PRIORITY_MAP.equals(priority);
            // "if (maps.containsKey(tId))" below should be almost always true.
            // hence this while loop would almost always have O(1) complexity
            String host = allocated.getNodeId().getHost();
            LinkedList<TaskAttemptId> list = mapsLocationMappingStorageType
                  .getLocationStorageTypeTaskAttemptIDs(host,
                        MapsLocationMappingStorageType.Location.HOSTLOCAL);
            while (list != null && list.size() > 0) {
              TaskAttemptId tId = list.removeFirst();
              if (requests.maps.containsKey(tId)) {
                ContainerRequest assigned = requests.maps.remove(tId);
                requests.containerAssigned(allocated, assigned);
                it.remove();
                JobCounterUpdateEvent jce =
                  new JobCounterUpdateEvent(assigned.attemptID.getTaskId().getJobId());
                jce.addCounterUpdate(JobCounter.DATA_LOCAL_MAPS, 1);
                eventHandler.handle(jce);
                hostLocalAssigned++;
                LOG.info("Assigned task " + tId + " based on host match " + host
                      + " with storage type " 
                      + mapsLocationMappingStorageType.getLastStorageTypeAccessed());
                break;
              }
              list = mapsLocationMappingStorageType.getLocationStorageTypeTaskAttemptIDs(host,
                                                    MapsLocationMappingStorageType.Location.HOSTLOCAL);
            }
          }
          
          // try to match all rack local
          it = allocatedContainers.iterator();
          while(it.hasNext() && requests.maps.size() > 0 && canAssignMaps()){
            Container allocated = it.next();
            Priority priority = allocated.getPriority();
            assert PRIORITY_MAP.equals(priority);
            // "if (maps.containsKey(tId))" below should be almost always true.
            // hence this while loop would almost always have O(1) complexity
            String host = allocated.getNodeId().getHost();
            String rack = resolveRack(host);
            LinkedList<TaskAttemptId> list = mapsLocationMappingStorageType
                  .getLocationStorageTypeTaskAttemptIDs(rack,
                        MapsLocationMappingStorageType.Location.RACKLOCAL);
            while (list != null && list.size() > 0) {
              TaskAttemptId tId = list.removeFirst();
              if (requests.maps.containsKey(tId)) {
                ContainerRequest assigned = requests.maps.remove(tId);
                requests.containerAssigned(allocated, assigned);
                it.remove();
                JobCounterUpdateEvent jce =
                  new JobCounterUpdateEvent(assigned.attemptID.getTaskId().getJobId());
                jce.addCounterUpdate(JobCounter.RACK_LOCAL_MAPS, 1);
                eventHandler.handle(jce);
                rackLocalAssigned++;
                LOG.info("Assigned task " + tId + " based on rack match " + rack
                      + " (host=" + host + ") with storage type " 
                      + mapsLocationMappingStorageType.getLastStorageTypeAccessed());
                break;
              }
              list = mapsLocationMappingStorageType.getLocationStorageTypeTaskAttemptIDs(rack,
                    MapsLocationMappingStorageType.Location.RACKLOCAL);
            }
          }
          
          // assign remaining
          it = allocatedContainers.iterator();
          while(it.hasNext() && requests.maps.size() > 0 && canAssignMaps()){
            Container allocated = it.next();
            Priority priority = allocated.getPriority();
            assert PRIORITY_MAP.equals(priority);
            TaskAttemptId tId = requests.maps.keySet().iterator().next();
            ContainerRequest assigned = requests.maps.remove(tId);
            requests.containerAssigned(allocated, assigned);
            it.remove();
            JobCounterUpdateEvent jce =
              new JobCounterUpdateEvent(assigned.attemptID.getTaskId().getJobId());
            jce.addCounterUpdate(JobCounter.OTHER_LOCAL_MAPS, 1);
            eventHandler.handle(jce);
            LOG.info("Assigned task " + assigned.attemptID 
                  + " on host=" + allocated.getNodeId().getHost() 
                  + " based on * match");
          }
      }
  }

  /**
   * An assignment solver maps the assignment problem into an unbalanced assignment
   * problem and uses the Hungarian algorithm to find the optimal solution.
   * 
   * Problem: The problem instance has a number of container requests and a number of
   * allocated containers. Any allocated container can be assigned to satisfy a request
   * but incurs some cost that depends on the locality and storage type of the assignment.
   * It is required to satisfy as many requests as possible by assigning at most one 
   * container to each request and at most one request to each container, in such a way 
   * that the total cost of the assignment is minimized.
   * 
   * @author hero
   */
  class AMAssignmentSolverHungarian extends AMAssignmentSolver {
    
    @Override
    public void addContainerRequest(TaskAttemptId id, ContainerRequest req) {
        // nothing to do
    }

    @Override
    public void updateContainerRequest(TaskAttemptId id, ContainerRequest req) {
        // nothing to do
    }

    @Override
    public TaskAttemptId getReplacementTaskAttemptId(Container allocated,
            ScheduledRequests requests) {
        
        String host = allocated.getNodeId().getHost();
        for (ContainerRequest request : requests.maps.values()) {
            if (request.types.containsKey(host))
                return request.attemptID;
        }
        
        return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void assignContainers(List<Container> allocatedContainers, ScheduledRequests requests) {

        // Basic checks
        if (allocatedContainers.isEmpty() || requests.maps.isEmpty() || !canAssignMaps())
            return;
        
        List<ContainerRequest> contRequests = new ArrayList<RMContainerRequestor.ContainerRequest>(requests.maps.values());
        
        // Initialize the cost matrix
        int numAlloc = allocatedContainers.size();
        int numReq = contRequests.size();
        int[][] costMatrix = new int[numAlloc][numReq];
        
        for (int a = 0; a < numAlloc; ++a) {
            for (int r = 0; r < numReq; ++r) {
                costMatrix[a][r] = computeCost(allocatedContainers.get(a), contRequests.get(r));
            }
        }
        
        // Run the Hungarian algorithm
        LOG.info("Running Hungarian algorithm with " + numAlloc + 
              " allocations and " + numReq + " container requests" );
        HungarianAlgorithm algo = new HungarianAlgorithm(costMatrix);
        int[] results = algo.execute();
        
        HashSet<ContainerId> toBeDeleted = new HashSet<ContainerId>(numAlloc);
        String host, rack;
        
        for (int a = 0; a < results.length; ++a) {
            if (results[a] >= 0) {
                // Make the assignment
                Container allocated = allocatedContainers.get(a);
                ContainerRequest assigned = contRequests.get(results[a]);
                
                requests.maps.remove(assigned.attemptID);
                requests.containerAssigned(allocated, assigned);
                toBeDeleted.add(allocated.getId());
                
                JobCounterUpdateEvent jce =
                  new JobCounterUpdateEvent(assigned.attemptID.getTaskId().getJobId());
                host = allocated.getNodeId().getHost();
                if (costMatrix[a][results[a]] < COST_RACK_LOCAL) {
                    // Assignment was node-local
                    jce.addCounterUpdate(JobCounter.DATA_LOCAL_MAPS, 1);
                    hostLocalAssigned++;
                    LOG.info("Assigned task " + assigned.attemptID 
                            + " based on host match " + host
                            + " with storage type " + assigned.types.get(host));
                } else if (costMatrix[a][results[a]] < COST_OFF_SWITCH) {
                    // Assignment was rack-local
                    jce.addCounterUpdate(JobCounter.RACK_LOCAL_MAPS, 1);
                    rackLocalAssigned++;
                    rack = resolveRack(host);
                    LOG.info("Assigned task " + assigned.attemptID 
                            + " based on rack match " + rack + " (host=" + host 
                            + ") with storage type " + assigned.types.get(rack));
                } else {
                    // Assignment was off switch
                    jce.addCounterUpdate(JobCounter.OTHER_LOCAL_MAPS, 1);
                    LOG.info("Assigned task " + assigned.attemptID 
                            + " on host=" + host + " based on * match");
                }
                
                eventHandler.handle(jce);
                
                if (!canAssignMaps())
                    break;
            } else {
                LOG.info("Unable to assign container " + allocatedContainers.get(a)
                        + " using the Hungarian Algorithm");
            }
        }
        
        // Delete allocated containers that were assigned successfully
        if (allocatedContainers.size() == toBeDeleted.size()) {
            allocatedContainers.clear();
        } else {
            Iterator<Container> it = allocatedContainers.iterator();
            while(it.hasNext()){
                Container allocated = it.next();
                if (toBeDeleted.contains(allocated.getId())) {
                    it.remove();
                }
            }
        }
    }
    
    /**
     * Compute the cost of assigning the allocated container to the request
     * 
     * @param container
     * @param containerRequest
     * @return
     */
    private int computeCost(Container container, ContainerRequest request) {
        
        String host = container.getNodeId().getHost();
        if (request.types.containsKey(host)) {
            // node-local cost
            if (isTierAware())
                return COST_NODE_LOCAL + request.types.get(host).getOrderValue();
            else
                return COST_NODE_LOCAL;
        }
        
        String rack = resolveRack(host);
        if (request.types.containsKey(rack)) {
            // rack-local cost
            if (isTierAware())
                return COST_RACK_LOCAL + request.types.get(rack).getOrderValue();
            else
                return COST_RACK_LOCAL;
        }
        
        // off switch cost
        return COST_OFF_SWITCH;
    }
  }

}
