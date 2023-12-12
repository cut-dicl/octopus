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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.hungarian;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt.ContainersAndNMTokensAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerExpiredSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeResourceUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.server.utils.Lock;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.annotations.VisibleForTesting;

@LimitedPrivate("yarn")
@Evolving
@SuppressWarnings("unchecked")
public class HungarianScheduler extends AbstractYarnScheduler<FiCaSchedulerApp, FiCaSchedulerNode>
      implements Configurable {

   private static final Log LOG = LogFactory.getLog(HungarianScheduler.class);

   private static final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

   private Configuration conf;

   private boolean usePortForNodeName;

   private ActiveUsersManager activeUsersManager;

   private static final String DEFAULT_QUEUE_NAME = "default";
   private QueueMetrics metrics;

   private final ResourceCalculator resourceCalculator = new DefaultResourceCalculator();

   private final HashMap<String, FiCaSchedulerNode> hostnameToNode = new HashMap<String, FiCaSchedulerNode>();
   private final HashMap<String, ArrayList<FiCaSchedulerNode>> racknameToNodes = new HashMap<String, ArrayList<FiCaSchedulerNode>>();

   private final Queue DEFAULT_QUEUE = new Queue() {
      @Override
      public String getQueueName() {
         return DEFAULT_QUEUE_NAME;
      }

      @Override
      public QueueMetrics getMetrics() {
         return metrics;
      }

      @Override
      public QueueInfo getQueueInfo(boolean includeChildQueues, boolean recursive) {
         QueueInfo queueInfo = recordFactory.newRecordInstance(QueueInfo.class);
         queueInfo.setQueueName(DEFAULT_QUEUE.getQueueName());
         queueInfo.setCapacity(1.0f);
         if (clusterResource.getMemory() == 0) {
            queueInfo.setCurrentCapacity(0.0f);
         } else {
            queueInfo.setCurrentCapacity(
                  (float) usedResource.getMemory() / clusterResource.getMemory());
         }
         queueInfo.setMaximumCapacity(1.0f);
         queueInfo.setChildQueues(new ArrayList<QueueInfo>());
         queueInfo.setQueueState(QueueState.RUNNING);
         return queueInfo;
      }

      public Map<QueueACL, AccessControlList> getQueueAcls() {
         Map<QueueACL, AccessControlList> acls = new HashMap<QueueACL, AccessControlList>();
         for (QueueACL acl : QueueACL.values()) {
            acls.put(acl, new AccessControlList("*"));
         }
         return acls;
      }

      @Override
      public List<QueueUserACLInfo> getQueueUserAclInfo(UserGroupInformation unused) {
         QueueUserACLInfo queueUserAclInfo = recordFactory
               .newRecordInstance(QueueUserACLInfo.class);
         queueUserAclInfo.setQueueName(DEFAULT_QUEUE_NAME);
         queueUserAclInfo.setUserAcls(Arrays.asList(QueueACL.values()));
         return Collections.singletonList(queueUserAclInfo);
      }

      @Override
      public boolean hasAccess(QueueACL acl, UserGroupInformation user) {
         return getQueueAcls().get(acl).isUserAllowed(user);
      }

      @Override
      public ActiveUsersManager getActiveUsersManager() {
         return activeUsersManager;
      }

      @Override
      public void recoverContainer(Resource clusterResource,
            SchedulerApplicationAttempt schedulerAttempt, RMContainer rmContainer) {
         if (rmContainer.getState().equals(RMContainerState.COMPLETED)) {
            return;
         }
         increaseUsedResources(rmContainer);
         updateAppHeadRoom(schedulerAttempt);
         updateAvailableResourcesMetrics();
      }

      @Override
      public Set<String> getAccessibleNodeLabels() {
         return null;
      }

      @Override
      public String getDefaultNodeLabelExpression() {
         return null;
      }
   };

   // Asynchronous scheduling parameters
   private static final String ASYNC_SCHEDULER_INTERVAL = "yarn.scheduler.hungarian.scheduling-interval-ms";
   private static final long DEFAULT_ASYNC_SCHEDULER_INTERVAL = 300;

   private static final String NODE_LOCALITY_DELAY = "yarn.scheduler.hungarian.node-locality-delay";
   private static final int DEFAULT_NODE_LOCALITY_DELAY = 3;

   private static final String ENABLE_TIERS = "yarn.scheduler.hungarian.enable-tiers";
   private static final boolean DEFAULT_ENABLE_TIERS = true;

   private static final Integer COST_NODE_LOCAL = 0;
   private static final Integer COST_RACK_LOCAL = 10;
   private static final Integer COST_OFF_SWITCH = 20;

   private AsyncScheduleThread asyncSchedulerThread;
   private long asyncScheduleInterval;
   private AtomicInteger numNodeManagers = new AtomicInteger(0);
   private int nodeLocalityDelay;
   private boolean enableTiers;

   /**
    * Thread to drive asynchronous scheduling
    */
   static class AsyncScheduleThread extends Thread {

      private final HungarianScheduler scheduler;
      private AtomicBoolean runSchedules = new AtomicBoolean(false);

      public AsyncScheduleThread(HungarianScheduler scheduler) {
         this.scheduler = scheduler;
         setDaemon(true);
      }

      @Override
      public void run() {
         while (true) {
            if (!runSchedules.get()) {
               try {
                  Thread.sleep(100);
               } catch (InterruptedException ie) {
               }
            } else {
               scheduler.scheduleAsync();

               try {
                  Thread.sleep(scheduler.getAsyncScheduleInterval());
               } catch (InterruptedException e) {
               }
            }
         }
      }

      public void beginSchedule() {
         runSchedules.set(true);
      }

      public void suspendSchedule() {
         runSchedules.set(false);
      }

   }

   long getAsyncScheduleInterval() {
      return asyncScheduleInterval;
   }

   public HungarianScheduler() {
      super(HungarianScheduler.class.getName());
   }

   private synchronized void initScheduler(Configuration conf) {
      validateConf(conf);
      // Use ConcurrentSkipListMap because applications need to be ordered
      this.applications = new ConcurrentSkipListMap<ApplicationId, SchedulerApplication<FiCaSchedulerApp>>();
      this.minimumAllocation = Resources
            .createResource(conf.getInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
                  YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB));
      initMaximumResourceCapability(Resources.createResource(
            conf.getInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
                  YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB),
            conf.getInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
                  YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES)));
      this.usePortForNodeName = conf.getBoolean(
            YarnConfiguration.RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_USE_PORT_FOR_NODE_NAME);
      this.metrics = QueueMetrics.forQueue(DEFAULT_QUEUE_NAME, null, false, conf);
      this.activeUsersManager = new ActiveUsersManager(metrics);

      this.asyncScheduleInterval = conf.getLong(ASYNC_SCHEDULER_INTERVAL,
            DEFAULT_ASYNC_SCHEDULER_INTERVAL);
      this.asyncSchedulerThread = new AsyncScheduleThread(this);

      this.nodeLocalityDelay = conf.getInt(NODE_LOCALITY_DELAY, DEFAULT_NODE_LOCALITY_DELAY);
      this.enableTiers = conf.getBoolean(ENABLE_TIERS, DEFAULT_ENABLE_TIERS);

      LOG.info("Initialized HungarianScheduler with " + "calculator="
            + getResourceCalculator().getClass() + ", " + "minimumAllocation=<"
            + getMinimumResourceCapability() + ">, " + "maximumAllocation=<"
            + getMaximumResourceCapability() + ">, " + "asyncScheduleInterval="
            + asyncScheduleInterval + "ms, rackLocalityDelay=" + nodeLocalityDelay);
   }

   @Override
   public void serviceInit(Configuration conf) throws Exception {
      initScheduler(conf);
      super.serviceInit(conf);
   }

   @Override
   public void serviceStart() throws Exception {
      if (asyncSchedulerThread != null) {
         asyncSchedulerThread.start();
      }
      super.serviceStart();
   }

   @Override
   public void serviceStop() throws Exception {
      synchronized (this) {
         if (asyncSchedulerThread != null) {
            asyncSchedulerThread.interrupt();
            asyncSchedulerThread.join(1000);
         }
      }
      super.serviceStop();
   }

   @Override
   public synchronized void setConf(Configuration conf) {
      this.conf = conf;
   }

   private void validateConf(Configuration conf) {
      // validate scheduler memory allocation setting
      int minMem = conf.getInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
      int maxMem = conf.getInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);

      if (minMem <= 0 || minMem > maxMem) {
         throw new YarnRuntimeException("Invalid resource scheduler memory"
               + " allocation configuration" + ", "
               + YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB + "=" + minMem + ", "
               + YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB + "=" + maxMem
               + ", min and max should be greater than 0" + ", max should be no smaller than min.");
      }
   }

   @Override
   public synchronized Configuration getConf() {
      return conf;
   }

   @Override
   public int getNumClusterNodes() {
      return nodes.size();
   }

   @Override
   public synchronized void setRMContext(RMContext rmContext) {
      this.rmContext = rmContext;
   }

   @Override
   public synchronized void reinitialize(Configuration conf, RMContext rmContext)
         throws IOException {
      setConf(conf);
   }

   @Override
   public Allocation allocate(ApplicationAttemptId applicationAttemptId, List<ResourceRequest> ask,
         List<ContainerId> release, List<String> blacklistAdditions,
         List<String> blacklistRemovals) {
      FiCaSchedulerApp application = getApplicationAttempt(applicationAttemptId);
      if (application == null) {
         LOG.error("Calling allocate on removed " + "or non existant application "
               + applicationAttemptId);
         return EMPTY_ALLOCATION;
      }

      // Sanity check
      SchedulerUtils.normalizeRequests(ask, resourceCalculator, clusterResource, minimumAllocation,
            getMaximumResourceCapability());

      // Release containers
      releaseContainers(release, application);

      synchronized (application) {

         // make sure we aren't stopping/removing the application
         // when the allocate comes in
         if (application.isStopped()) {
            LOG.info("Calling allocate on a stopped " + "application " + applicationAttemptId);
            return EMPTY_ALLOCATION;
         }

         if (!ask.isEmpty()) {
            LOG.debug("allocate: pre-update" + " applicationId=" + applicationAttemptId
                  + " application=" + application);
            application.showRequests();

            // Update application requests
            application.updateResourceRequests(ask);

            LOG.debug("allocate: post-update" + " applicationId=" + applicationAttemptId
                  + " application=" + application);
            application.showRequests();

            LOG.info(
                  "allocate:" + " applicationId=" + applicationAttemptId + " #ask=" + ask.size());
         }

         application.updateBlacklist(blacklistAdditions, blacklistRemovals);
         ContainersAndNMTokensAllocation allocation = application
               .pullNewlyAllocatedContainersAndNMTokens();
         Resource headroom = application.getHeadroom();
         application.setApplicationHeadroomForMetrics(headroom);
         return new Allocation(allocation.getContainerList(), headroom, null, null, null,
               allocation.getNMTokenList());
      }
   }

   private FiCaSchedulerNode getNode(NodeId nodeId) {
      return nodes.get(nodeId);
   }

   @VisibleForTesting
   public synchronized void addApplication(ApplicationId applicationId, String queue, String user,
         boolean isAppRecovering) {
      SchedulerApplication<FiCaSchedulerApp> application = new SchedulerApplication<FiCaSchedulerApp>(
            DEFAULT_QUEUE, user);
      applications.put(applicationId, application);
      metrics.submitApp(user);
      LOG.info("Accepted application " + applicationId + " from user: " + user
            + ", currently num of applications: " + applications.size());
      if (isAppRecovering) {
         if (LOG.isDebugEnabled()) {
            LOG.debug(applicationId + " is recovering. Skip notifying APP_ACCEPTED");
         }
      } else {
         rmContext.getDispatcher().getEventHandler()
               .handle(new RMAppEvent(applicationId, RMAppEventType.APP_ACCEPTED));
      }
   }

   @VisibleForTesting
   public synchronized void addApplicationAttempt(ApplicationAttemptId appAttemptId,
         boolean transferStateFromPreviousAttempt, boolean isAttemptRecovering) {
      SchedulerApplication<FiCaSchedulerApp> application = applications
            .get(appAttemptId.getApplicationId());
      String user = application.getUser();

      FiCaSchedulerApp schedulerApp = new FiCaSchedulerApp(appAttemptId, user, DEFAULT_QUEUE,
            activeUsersManager, this.rmContext);

      if (transferStateFromPreviousAttempt) {
         schedulerApp.transferStateFromPreviousAttempt(application.getCurrentAppAttempt());
      }
      application.setCurrentAppAttempt(schedulerApp);

      metrics.submitAppAttempt(user);
      LOG.info("Added Application Attempt " + appAttemptId + " to scheduler from user "
            + application.getUser());
      if (isAttemptRecovering) {
         if (LOG.isDebugEnabled()) {
            LOG.debug(appAttemptId + " is recovering. Skipping notifying ATTEMPT_ADDED");
         }
      } else {
         rmContext.getDispatcher().getEventHandler()
               .handle(new RMAppAttemptEvent(appAttemptId, RMAppAttemptEventType.ATTEMPT_ADDED));
      }
   }

   private synchronized void doneApplication(ApplicationId applicationId, RMAppState finalState) {
      SchedulerApplication<FiCaSchedulerApp> application = applications.get(applicationId);
      if (application == null) {
         LOG.warn("Couldn't find application " + applicationId);
         return;
      }

      // Inform the activeUsersManager
      activeUsersManager.deactivateApplication(application.getUser(), applicationId);
      application.stop(finalState);
      applications.remove(applicationId);
   }

   private synchronized void doneApplicationAttempt(ApplicationAttemptId applicationAttemptId,
         RMAppAttemptState rmAppAttemptFinalState, boolean keepContainers) throws IOException {
      FiCaSchedulerApp attempt = getApplicationAttempt(applicationAttemptId);
      SchedulerApplication<FiCaSchedulerApp> application = applications
            .get(applicationAttemptId.getApplicationId());
      if (application == null || attempt == null) {
         throw new IOException("Unknown application " + applicationAttemptId + " has completed!");
      }

      // Kill all 'live' containers
      for (RMContainer container : attempt.getLiveContainers()) {
         if (keepContainers && container.getState().equals(RMContainerState.RUNNING)) {
            // do not kill the running container in the case of work-preserving AM
            // restart.
            LOG.info("Skip killing " + container.getContainerId());
            continue;
         }
         completedContainer(container,
               SchedulerUtils.createAbnormalContainerStatus(container.getContainerId(),
                     SchedulerUtils.COMPLETED_APPLICATION),
               RMContainerEventType.KILL);
      }

      // Clean up pending requests, metrics etc.
      attempt.stop(rmAppAttemptFinalState);
   }

   /**
    * Main method to be called periodically by the scheduling thread. For each
    * application and for each priority, run the Hungarian algorithm for assigning
    * as many containers as possible.
    */
   public synchronized void scheduleAsync() {

      if (rmContext.isWorkPreservingRecoveryEnabled()
            && !rmContext.isSchedulerReadyForAllocatingContainers()) {
         return;
      }

      // Try to assign containers to applications in fifo order
      for (Map.Entry<ApplicationId, SchedulerApplication<FiCaSchedulerApp>> e : applications
            .entrySet()) {
         FiCaSchedulerApp application = e.getValue().getCurrentAppAttempt();
         if (application == null) {
            continue;
         }

         application.showRequests();
         synchronized (application) {
            for (Priority priority : application.getPriorities()) {
               ResourceRequest offSwitchRequest = application.getResourceRequest(priority,
                     ResourceRequest.ANY);
               if (offSwitchRequest != null && offSwitchRequest.getNumContainers() > 0) {
                  int assignedContainers = assignContainers(application, priority,
                        offSwitchRequest);

                  // Do not assign out of order w.r.t priorities
                  if (assignedContainers == 0) {
                     break;
                  }
               }
            }
         }

         application.showRequests();

         // Done
         if (Resources.lessThan(resourceCalculator, clusterResource,
               Resources.subtract(clusterResource, usedResource), minimumAllocation)) {
            break;
         }
      }

      // Update the applications' headroom to correctly take into
      // account the containers assigned in this update.
      for (SchedulerApplication<FiCaSchedulerApp> application : applications.values()) {
         FiCaSchedulerApp attempt = (FiCaSchedulerApp) application.getCurrentAppAttempt();
         if (attempt != null) {
            updateAppHeadRoom(attempt);
         }
      }
   }

   /**
    * Heart of the scheduler. This method uses the Hungarian algorithm to assign
    * containers to the request made with this priority by this application.
    * 
    * @param application
    * @param priority
    * @param offSwitchRequest
    * @return
    */
   private int assignContainers(FiCaSchedulerApp application, Priority priority,
         ResourceRequest offSwitchRequest) {

      Map<String, ResourceRequest> requests = application.getResourceRequests(priority);
      if (requests.size() == 1) {
         // There is only an off-switch request for this priority
         return assignContainersRandomly(application, priority, offSwitchRequest);
      }

      Resource capability = offSwitchRequest.getCapability();
      int offSwitchContainers = offSwitchRequest.getNumContainers();
      int assignableContainers, availableContainers, maxContainers;

      ArrayList<PotentialContainer> potentialContainers = new ArrayList<PotentialContainer>();
      ArrayList<ResourceRequest> nonLocalRequests = new ArrayList<ResourceRequest>(3);
      HashMap<String, Integer> extraContainers = new HashMap<String, Integer>();

      int maxAvailableContainers = resourceCalculator.computeAvailableContainers(
            Resources.subtract(clusterResource, usedResource), capability);

      if (maxAvailableContainers > 0) {
         // Search for local hosts to satisfy requests
         for (ResourceRequest request : requests.values()) {
            if (hostnameToNode.containsKey(request.getResourceName())) {
               FiCaSchedulerNode node = hostnameToNode.get(request.getResourceName());

               // Compute how many containers we can potentially allocate on the node
               assignableContainers = getMaxAssignableContainers(application, node,
                     offSwitchRequest, requests.get(node.getRMNode().getRackName()), request);
               availableContainers = resourceCalculator
                     .computeAvailableContainers(node.getAvailableResource(), capability);
               maxContainers = Math.min(assignableContainers, availableContainers);

               PreferencesIterator iter = enableTiers
                     ? new PreferencesIterator(request.getPreferences())
                     : null;

               for (int i = 0; i < maxContainers; ++i) {
                  potentialContainers.add(
                        new PotentialContainer(node, enableTiers ? iter.next() : COST_NODE_LOCAL));
               }

               extraContainers.put(node.getNodeName(),
                     (assignableContainers == 0) ? 0 : availableContainers - assignableContainers);
            } else {
               nonLocalRequests.add(request);
            }
         }
      }

      if (potentialContainers.size() < offSwitchContainers
            && maxAvailableContainers - potentialContainers.size() > 0
            && !delayNonLocal(application, priority)) {
         // Search for local racks to satisfy requests
         for (ResourceRequest request : nonLocalRequests) {
            if (racknameToNodes.containsKey(request.getResourceName())) {
               for (FiCaSchedulerNode node : racknameToNodes.get(request.getResourceName())) {
                  if (extraContainers.containsKey(node.getNodeName())) {
                     // Already used this node but it may have more containers
                     maxContainers = extraContainers.get(node.getNodeName());
                  } else {
                     // Compute how many containers we can potentially allocate
                     assignableContainers = getMaxAssignableContainers(application, node,
                           offSwitchRequest, request, null);
                     availableContainers = resourceCalculator
                           .computeAvailableContainers(node.getAvailableResource(), capability);
                     maxContainers = Math.min(assignableContainers, availableContainers);
                  }

                  PreferencesIterator iter = enableTiers
                        ? new PreferencesIterator(request.getPreferences())
                        : null;

                  for (int i = 0; i < maxContainers; ++i) {
                     potentialContainers.add(new PotentialContainer(node,
                           enableTiers ? COST_RACK_LOCAL + iter.next() : COST_RACK_LOCAL));
                  }

                  extraContainers.put(node.getNodeName(), 0);

                  if (potentialContainers.size() >= offSwitchContainers
                        || maxAvailableContainers - potentialContainers.size() <= 0) {
                     break;
                  }
               }
            }
         }
      }

      if (potentialContainers.size() < offSwitchContainers
            && maxAvailableContainers - potentialContainers.size() > 0 && racknameToNodes.size() > 1
            && !delayNonLocal(application, priority)) {
         // Search for remote racks to satisfy requests
         for (FiCaSchedulerNode node : this.nodes.values()) {
            if (!extraContainers.containsKey(node.getNodeName())) {
               // Compute how many containers we can potentially allocate
               assignableContainers = getMaxAssignableContainers(application, node,
                     offSwitchRequest, offSwitchRequest, null);
               availableContainers = resourceCalculator
                     .computeAvailableContainers(node.getAvailableResource(), capability);
               maxContainers = Math.min(assignableContainers, availableContainers);

               for (int i = 0; i < maxContainers; ++i) {
                  potentialContainers.add(new PotentialContainer(node, COST_OFF_SWITCH));
               }

               if (potentialContainers.size() >= offSwitchContainers
                     || maxAvailableContainers - potentialContainers.size() <= 0) {
                  break;
               }
            }
         }
      }

      if (potentialContainers.size() == 0) {
         // Not enough resources for this job
         return 0;
      }

      // Compute the assignments
      int numPotContainers = potentialContainers.size();
      int numRequests = Math.min(offSwitchContainers, numPotContainers);

      if (numPotContainers != numRequests) {
         // Shuffle first to mix up containers across nodes; then sort based on cost
         Collections.shuffle(potentialContainers);
         Collections.sort(potentialContainers);
      }

      // Side note: The below is not needed because the constructed table
      // would have the same values for all rows. Sorting the first row
      // yields the same results as applying the HungarianAlgorithm
      // int[] results;
      // if (numPotContainers == numRequests) {
      // // All containers will be assigned - no need to run the algorithm
      // results = new int[numRequests];
      // for (int r = 0; r < results.length; ++r)
      // results[r] = r;
      // } else {
      // // Run the Hungarian algorithm
      // int[][] costMatrix = new int[numRequests][numPotContainers];
      // for (int r = 0; r < numRequests; ++r) {
      // for (int p = 0; p < numPotContainers; ++p) {
      // costMatrix[r][p] = potentialContainers.get(p).cost;
      // }
      // }
      //
      // HungarianAlgorithm algo = new HungarianAlgorithm(costMatrix);
      // results = algo.execute();
      // }

      int assignedTotal = 0, assignedNodeLocal = 0, assignedRackLocal = 0, assignedOff = 0;
      PotentialContainer potContainer;

      for (int r = 0; r < numRequests; ++r) {
         // Make the assignment
         potContainer = potentialContainers.get(r);
         if (potContainer.cost < COST_RACK_LOCAL) {
            if (requests.containsKey(potContainer.node.getRackName())) {
               assignedTotal += assignContainer(potContainer.node, application, priority, 1,
                     requests.get(potContainer.node.getNodeName()), NodeType.NODE_LOCAL);
               assignedNodeLocal++;
            } else {
               // No more rack requests (happens when ANY requests > rack requests)
               assignedTotal += assignContainer(potContainer.node, application, priority, 1,
                     offSwitchRequest, NodeType.OFF_SWITCH);
               assignedOff++;
            }
         } else if (potContainer.cost < COST_OFF_SWITCH) {
            assignedTotal += assignContainer(potContainer.node, application, priority, 1,
                  requests.get(potContainer.node.getRackName()), NodeType.RACK_LOCAL);
            assignedRackLocal++;
         } else {
            assignedTotal += assignContainer(potContainer.node, application, priority, 1,
                  offSwitchRequest, NodeType.OFF_SWITCH);
            assignedOff++;
         }
      }

      if (assignedNodeLocal > 0) {
         application.resetSchedulingOpportunities(priority);
      }

      StringBuilder sb = new StringBuilder(160);
      sb.append("Assigned (");
      sb.append(assignedNodeLocal);
      sb.append(", ");
      sb.append(assignedRackLocal);
      sb.append(", ");
      sb.append(assignedOff);
      sb.append(") ");
      sb.append(assignedTotal);
      sb.append(" containers out of ");
      sb.append(offSwitchContainers);
      sb.append(" requests and ");
      sb.append(potentialContainers.size());
      sb.append(" potential options for application attempt ");
      sb.append(application.getApplicationAttemptId());
      sb.append(" and priority ");
      sb.append(priority);
      LOG.info(sb.toString());

      return assignedTotal;
   }

   /**
    * If the scheduler goes through 'nodeLocalityDelay' intervals without
    * scheduling any node local containers, then allows for non-local assignments
    * 
    * @param application
    * @param priority
    * @return
    */
   private boolean delayNonLocal(FiCaSchedulerApp application, Priority priority) {
      application.addSchedulingOpportunity(priority);
      if (application.getSchedulingOpportunities(priority) <= nodeLocalityDelay) {
         LOG.info("Delaying non-local assignment for " + application.getApplicationAttemptId());
         return true;
      } else {
         return false;
      }
   }

   /**
    * Assign containers for this application/priority in random nodes. An attempt
    * is made to balance the assignment across the nodes. This method is used for
    * satisfying requests that do not have any locality associated with them (e.g.,
    * for reduce tasks).
    * 
    * @param application
    * @param priority
    * @param offSwitchRequest
    * @return the number of assigned containers
    */
   private int assignContainersRandomly(FiCaSchedulerApp application, Priority priority,
         ResourceRequest offSwitchRequest) {

      // Compute an avg num of containers per node to spread the load
      Resource capability = offSwitchRequest.getCapability();
      int assignedContainers = 0;
      int requestedContainers = offSwitchRequest.getNumContainers();
      int containersPerNode = requestedContainers / nodes.size();
      containersPerNode = (containersPerNode == 0) ? 1 : containersPerNode;

      // Randomize the node order
      ArrayList<NodeId> nodeIds = new ArrayList<NodeId>(nodes.keySet());
      Collections.shuffle(nodeIds);

      // Make 2 rounds of assignments to maximize load balancing
      // First round with 'containersPerNode' and second round with 1 container per node
      for (int i = 0; i < 2 && offSwitchRequest.getNumContainers() > 0; ++i) {
          for (NodeId nodeId : nodeIds) {
             FiCaSchedulerNode node = nodes.get(nodeId);
             if (!SchedulerAppUtils.isBlacklisted(application, node, LOG)
                   && Resources.greaterThanOrEqual(resourceCalculator, clusterResource,
                         node.getAvailableResource(), capability)) {
                // Make the assignment
                assignedContainers += assignContainer(node, application, priority,
                      Math.min(containersPerNode, offSwitchRequest.getNumContainers()),
                      offSwitchRequest, NodeType.OFF_SWITCH);

                if (offSwitchRequest.getNumContainers() <= 0)
                   break;
             }
          }
          
          containersPerNode = 1;
      }

      if (offSwitchRequest.getNumContainers() > 0 && Resources.greaterThan(resourceCalculator,
            clusterResource, Resources.subtract(clusterResource, usedResource), capability)) {
         // It is possible there are still available resources in the cluster
         // Make one more round of assignments
         for (NodeId nodeId : nodeIds) {
            FiCaSchedulerNode node = nodes.get(nodeId);
            if (Resources.greaterThanOrEqual(resourceCalculator, clusterResource,
                  node.getAvailableResource(), capability)) {
               // Make the assignment
               assignedContainers += assignContainer(node, application, priority,
                     offSwitchRequest.getNumContainers(), offSwitchRequest, NodeType.OFF_SWITCH);

               if (offSwitchRequest.getNumContainers() <= 0)
                  break;
            }
         }
      }

      LOG.info("Assigned " + assignedContainers + " containers out of " + requestedContainers
            + " requests without locality info for Application attempt "
            + application.getApplicationAttemptId() + " and priority " + priority);

      return assignedContainers;
   }

   /**
    * Get the max containers requested as min of {node_local, rack_local,
    * off_switch}
    * 
    * @param application
    * @param node
    * @param offSwitchRequest
    * @param rackRequest
    * @param localRequest
    * @return
    */
   private int getMaxAssignableContainers(FiCaSchedulerApp application, FiCaSchedulerNode node,
         ResourceRequest offSwitchRequest, ResourceRequest rackRequest,
         ResourceRequest localRequest) {
      // Check if this resource is on the blacklist
      if (SchedulerAppUtils.isBlacklisted(application, node, LOG)) {
         return 0;
      }

      if (offSwitchRequest == null || rackRequest == null) {
         return 0;
      }

      int maxContainers = Math.min(offSwitchRequest.getNumContainers(),
            rackRequest.getNumContainers());
      if (localRequest == null)
         return maxContainers;
      else
         return Math.min(maxContainers, localRequest.getNumContainers());
   }

   /**
    * Assign a number of containers on the provided node based on the resource
    * request made by the provided application with the provided priority.
    * 
    * @param node
    * @param application
    * @param priority
    * @param assignableContainers
    * @param request
    * @param type
    * @return
    */
   private int assignContainer(FiCaSchedulerNode node, FiCaSchedulerApp application,
         Priority priority, int assignableContainers, ResourceRequest request, NodeType type) {
      LOG.debug("assignContainers:" + " node=" + node.getRMNode().getNodeAddress() + " application="
            + application.getApplicationId().getId() + " priority=" + priority.getPriority()
            + " assignableContainers=" + assignableContainers + " request=" + request + " type="
            + type);

      Resource capability = request.getCapability();

      int availableContainers = resourceCalculator
            .computeAvailableContainers(node.getAvailableResource(), capability);

      int assignedContainers = Math.min(assignableContainers, availableContainers);

      for (int i = 0; i < assignedContainers; ++i) {

         NodeId nodeId = node.getRMNode().getNodeID();
         ContainerId containerId = BuilderUtils.newContainerId(
               application.getApplicationAttemptId(), application.getNewContainerId());

         // Create the container
         Container container = BuilderUtils.newContainer(containerId, nodeId,
               node.getRMNode().getHttpAddress(), capability, priority, null);

         // Allocate!

         // Inform the application
         RMContainer rmContainer = application.allocate(type, node, priority, request, container);

         // Inform the node
         node.allocateContainer(rmContainer);

         // Update usage for this container
         increaseUsedResources(rmContainer);
      }

      return assignedContainers;
   }

   private synchronized void nodeUpdate(RMNode rmNode) {
      FiCaSchedulerNode node = getNode(rmNode.getNodeID());

      List<UpdatedContainerInfo> containerInfoList = rmNode.pullContainerUpdates();
      List<ContainerStatus> newlyLaunchedContainers = new ArrayList<ContainerStatus>();
      List<ContainerStatus> completedContainers = new ArrayList<ContainerStatus>();
      for (UpdatedContainerInfo containerInfo : containerInfoList) {
         newlyLaunchedContainers.addAll(containerInfo.getNewlyLaunchedContainers());
         completedContainers.addAll(containerInfo.getCompletedContainers());
      }
      // Processing the newly launched containers
      for (ContainerStatus launchedContainer : newlyLaunchedContainers) {
         containerLaunchedOnNode(launchedContainer.getContainerId(), node);
      }

      // Process completed containers
      for (ContainerStatus completedContainer : completedContainers) {
         ContainerId containerId = completedContainer.getContainerId();
         LOG.debug("Container FINISHED: " + containerId);
         completedContainer(getRMContainer(containerId), completedContainer,
               RMContainerEventType.FINISHED);
      }

      updateAvailableResourcesMetrics();
   }

   private void increaseUsedResources(RMContainer rmContainer) {
      Resources.addTo(usedResource, rmContainer.getAllocatedResource());
   }

   private void updateAppHeadRoom(SchedulerApplicationAttempt schedulerAttempt) {
      schedulerAttempt.setHeadroom(Resources.subtract(clusterResource, usedResource));
   }

   private void updateAvailableResourcesMetrics() {
      metrics.setAvailableResourcesToQueue(Resources.subtract(clusterResource, usedResource));
   }

   @Override
   public void handle(SchedulerEvent event) {
      switch (event.getType()) {
      case NODE_ADDED: {
         NodeAddedSchedulerEvent nodeAddedEvent = (NodeAddedSchedulerEvent) event;
         addNode(nodeAddedEvent.getAddedRMNode());
         recoverContainersOnNode(nodeAddedEvent.getContainerReports(),
               nodeAddedEvent.getAddedRMNode());

      }
         break;
      case NODE_REMOVED: {
         NodeRemovedSchedulerEvent nodeRemovedEvent = (NodeRemovedSchedulerEvent) event;
         removeNode(nodeRemovedEvent.getRemovedRMNode());
      }
         break;
      case NODE_RESOURCE_UPDATE: {
         NodeResourceUpdateSchedulerEvent nodeResourceUpdatedEvent = (NodeResourceUpdateSchedulerEvent) event;
         updateNodeResource(nodeResourceUpdatedEvent.getRMNode(),
               nodeResourceUpdatedEvent.getResourceOption());
      }
         break;
      case NODE_UPDATE: {
         NodeUpdateSchedulerEvent nodeUpdatedEvent = (NodeUpdateSchedulerEvent) event;
         nodeUpdate(nodeUpdatedEvent.getRMNode());
      }
         break;
      case APP_ADDED: {
         AppAddedSchedulerEvent appAddedEvent = (AppAddedSchedulerEvent) event;
         addApplication(appAddedEvent.getApplicationId(), appAddedEvent.getQueue(),
               appAddedEvent.getUser(), appAddedEvent.getIsAppRecovering());
      }
         break;
      case APP_REMOVED: {
         AppRemovedSchedulerEvent appRemovedEvent = (AppRemovedSchedulerEvent) event;
         doneApplication(appRemovedEvent.getApplicationID(), appRemovedEvent.getFinalState());
      }
         break;
      case APP_ATTEMPT_ADDED: {
         AppAttemptAddedSchedulerEvent appAttemptAddedEvent = (AppAttemptAddedSchedulerEvent) event;
         addApplicationAttempt(appAttemptAddedEvent.getApplicationAttemptId(),
               appAttemptAddedEvent.getTransferStateFromPreviousAttempt(),
               appAttemptAddedEvent.getIsAttemptRecovering());
      }
         break;
      case APP_ATTEMPT_REMOVED: {
         AppAttemptRemovedSchedulerEvent appAttemptRemovedEvent = (AppAttemptRemovedSchedulerEvent) event;
         try {
            doneApplicationAttempt(appAttemptRemovedEvent.getApplicationAttemptID(),
                  appAttemptRemovedEvent.getFinalAttemptState(),
                  appAttemptRemovedEvent.getKeepContainersAcrossAppAttempts());
         } catch (IOException ie) {
            LOG.error("Unable to remove application "
                  + appAttemptRemovedEvent.getApplicationAttemptID(), ie);
         }
      }
         break;
      case CONTAINER_EXPIRED: {
         ContainerExpiredSchedulerEvent containerExpiredEvent = (ContainerExpiredSchedulerEvent) event;
         ContainerId containerid = containerExpiredEvent.getContainerId();
         completedContainer(getRMContainer(containerid), SchedulerUtils
               .createAbnormalContainerStatus(containerid, SchedulerUtils.EXPIRED_CONTAINER),
               RMContainerEventType.EXPIRE);
      }
         break;
      default:
         LOG.error("Invalid eventtype " + event.getType() + ". Ignoring!");
      }
   }

   @Lock(HungarianScheduler.class)
   @Override
   protected synchronized void completedContainer(RMContainer rmContainer,
         ContainerStatus containerStatus, RMContainerEventType event) {
      if (rmContainer == null) {
         LOG.info("Null container completed...");
         return;
      }

      // Get the application for the finished container
      Container container = rmContainer.getContainer();
      FiCaSchedulerApp application = getCurrentAttemptForContainer(container.getId());
      ApplicationId appId = container.getId().getApplicationAttemptId().getApplicationId();

      // Get the node on which the container was allocated
      FiCaSchedulerNode node = getNode(container.getNodeId());

      if (application == null) {
         LOG.info("Unknown application: " + appId + " released container " + container.getId()
               + " on node: " + node + " with event: " + event);
         return;
      }

      // Inform the application
      application.containerCompleted(rmContainer, containerStatus, event);

      // Inform the node
      node.releaseContainer(container);

      // Update total usage
      Resources.subtractFrom(usedResource, container.getResource());

      LOG.info(
            "Application attempt " + application.getApplicationAttemptId() + " released container "
                  + container.getId() + " on node: " + node + " with event: " + event);

   }

   private Resource usedResource = recordFactory.newRecordInstance(Resource.class);

   private synchronized void removeNode(RMNode nodeInfo) {
      FiCaSchedulerNode node = getNode(nodeInfo.getNodeID());
      if (node == null) {
         return;
      }

      if (numNodeManagers.decrementAndGet() == 0) {
         asyncSchedulerThread.suspendSchedule();
      }

      // Kill running containers
      for (RMContainer container : node.getRunningContainers()) {
         completedContainer(container,
               SchedulerUtils.createAbnormalContainerStatus(container.getContainerId(),
                     SchedulerUtils.LOST_CONTAINER),
               RMContainerEventType.KILL);
      }

      // Remove the node
      this.nodes.remove(nodeInfo.getNodeID());
      this.hostnameToNode.remove(node.getNodeName());
      if (this.racknameToNodes.containsKey(node.getRackName())) {
         ArrayList<FiCaSchedulerNode> nodes = this.racknameToNodes.get(node.getRackName());
         nodes.remove(node);
         if (nodes.isEmpty())
            this.racknameToNodes.remove(node.getRackName());
      }
      updateMaximumAllocation(node, false);

      // Update cluster metrics
      Resources.subtractFrom(clusterResource, node.getRMNode().getTotalCapability());
   }

   @Override
   public QueueInfo getQueueInfo(String queueName, boolean includeChildQueues, boolean recursive) {
      return DEFAULT_QUEUE.getQueueInfo(false, false);
   }

   @Override
   public List<QueueUserACLInfo> getQueueUserAclInfo() {
      return DEFAULT_QUEUE.getQueueUserAclInfo(null);
   }

   @Override
   public ResourceCalculator getResourceCalculator() {
      return resourceCalculator;
   }

   private synchronized void addNode(RMNode nodeManager) {
      FiCaSchedulerNode schedulerNode = new FiCaSchedulerNode(nodeManager, usePortForNodeName);
      this.nodes.put(nodeManager.getNodeID(), schedulerNode);
      this.hostnameToNode.put(schedulerNode.getNodeName(), schedulerNode);
      if (!this.racknameToNodes.containsKey(schedulerNode.getRackName()))
         this.racknameToNodes.put(schedulerNode.getRackName(), new ArrayList<FiCaSchedulerNode>());
      this.racknameToNodes.get(schedulerNode.getRackName()).add(schedulerNode);

      Resources.addTo(clusterResource, nodeManager.getTotalCapability());
      updateMaximumAllocation(schedulerNode, true);

      if (numNodeManagers.incrementAndGet() == 1) {
         asyncSchedulerThread.beginSchedule();
      }
   }

   @Override
   public void recover(RMState state) {
      // NOT IMPLEMENTED
   }

   @Override
   public RMContainer getRMContainer(ContainerId containerId) {
      FiCaSchedulerApp attempt = getCurrentAttemptForContainer(containerId);
      return (attempt == null) ? null : attempt.getRMContainer(containerId);
   }

   @Override
   public QueueMetrics getRootQueueMetrics() {
      return DEFAULT_QUEUE.getMetrics();
   }

   @Override
   public synchronized boolean checkAccess(UserGroupInformation callerUGI, QueueACL acl,
         String queueName) {
      return DEFAULT_QUEUE.hasAccess(acl, callerUGI);
   }

   @Override
   public synchronized List<ApplicationAttemptId> getAppsInQueue(String queueName) {
      if (queueName.equals(DEFAULT_QUEUE.getQueueName())) {
         List<ApplicationAttemptId> attempts = new ArrayList<ApplicationAttemptId>(
               applications.size());
         for (SchedulerApplication<FiCaSchedulerApp> app : applications.values()) {
            attempts.add(app.getCurrentAppAttempt().getApplicationAttemptId());
         }
         return attempts;
      } else {
         return null;
      }
   }

   public Resource getUsedResource() {
      return usedResource;
   }

   @VisibleForTesting
   public void suspendSchedule() {
      // Used only during unit testing
      asyncSchedulerThread.suspendSchedule();
   }

   /**
    * A simple class to pair a node with a cost. In essence, it represents a
    * potential container on the node. The "cost" is a proxy for how good the
    * assignment would be for this container. For example, a memory-local
    * assignment would have a lower cost than a node-local assignment.
    * 
    * @author hero
    */
   private class PotentialContainer implements Comparable<PotentialContainer> {

      FiCaSchedulerNode node;
      Integer cost;

      PotentialContainer(FiCaSchedulerNode node, Integer cost) {
         this.node = node;
         this.cost = cost;
      }

      @Override
      public int compareTo(PotentialContainer o) {
         // Enable sorting only based on cost
         return this.cost.compareTo(o.cost);
      }

      @Override
      public String toString() {
         return node.getNodeName() + ": " + cost;
      }
   }

   /**
    * An iterator that returns the list of preferences based on the counts
    * appearing in the map (key=preference, value=count).
    * 
    * Example: Suppose the map contains {0=2, 3=1, 5=3}. This iterator will return
    * [0, 0, 3, 5, 5, 5]
    * 
    * Important note: If the next() method is called more times than there are
    * preferences (e.g., more than 6 times in the example above), then it will
    * return a default preference value. The hasNext() methods works as expected.
    * 
    * @author hero
    */
   static final class PreferencesIterator implements Iterator<Integer> {

      private static Integer defaultPref = new Integer(COST_RACK_LOCAL - 1);

      private Iterator<Entry<Integer, Integer>> iter;
      private Entry<Integer, Integer> currentEntry;
      private Integer currentPref;
      private int currentCount = 0;

      PreferencesIterator(NavigableMap<Integer, Integer> map) {
         iter = map.entrySet().iterator();
      }

      @Override
      public boolean hasNext() {
         return currentCount > 0 || iter.hasNext();
      }

      @Override
      public Integer next() {
         if (!hasNext()) {
            return defaultPref;
         }

         if (currentCount == 0) {
            currentEntry = iter.next();
            currentPref = currentEntry.getKey();
            currentCount = currentEntry.getValue();
         }
         currentCount--;

         return currentPref;
      }
   }
}
