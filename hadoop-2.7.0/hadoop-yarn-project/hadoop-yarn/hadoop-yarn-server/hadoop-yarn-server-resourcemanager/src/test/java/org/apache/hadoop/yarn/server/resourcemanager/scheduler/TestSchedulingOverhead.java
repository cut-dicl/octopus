package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.InlineDispatcher;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.ahs.RMApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.SystemMetricsPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.hungarian.HungarianScheduler;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * A class for measuring scheduler overheads
 * 
 * @author hero
 */
public class TestSchedulingOverhead {

   private static final Log LOG = LogFactory.getLog(TestSchedulingOverhead.class);

   private static final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

   public static void main(String[] args) throws IOException {

      TestSchedulingOverhead test = new TestSchedulingOverhead();
      Configuration conf;

      // Warm up
      conf = new Configuration();
      conf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class, ResourceScheduler.class);
      test.testAssignments(conf, -1, 8, 4, 2);

      int repeats = 3;
      int[] numTasks = { 16, 64, 256, 1024 };
      int[] numNodes = { 8, 32, 128, 512 };

      for (int r = 0; r < repeats; ++r) {
         for (int t = 0; t < numTasks.length; ++t) {
            for (int n = 0; n < numNodes.length; ++n) {
               // Test FIFO
               conf = new Configuration();
               conf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class,
                     ResourceScheduler.class);

               test.testAssignments(conf, r, numTasks[t], numNodes[n], 2);

               // Test Hungarian
               conf = new Configuration();
               conf.setClass(YarnConfiguration.RM_SCHEDULER, HungarianScheduler.class,
                     ResourceScheduler.class);
               conf.setLong("yarn.scheduler.hungarian.scheduling-interval-ms", 300);
               conf.setInt("yarn.scheduler.hungarian.node-locality-delay", 0);
               conf.setBoolean("yarn.scheduler.hungarian.enable-tiers", true);

               test.testAssignments(conf, r, numTasks[t], numNodes[n], 2);

               // Test Capacity
               conf = new Configuration();
               conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
                     ResourceScheduler.class);
               conf.setBoolean("yarn.scheduler.capacity.schedule-asynchronously.enable", true);
               conf.setLong("arn.scheduler.capacity.schedule-asynchronously.scheduling-interval-ms",
                     1);

               test.testAssignments(conf, r, numTasks[t], numNodes[n], 2);
            }
         }
      }

      // Done
      System.exit(0);
   }

   /**
    * Main experiment
    * 
    * @param conf
    * @param expID
    * @param numConts
    * @param numNodes
    * @param numCores
    * @throws IOException
    */
   public void testAssignments(Configuration conf, int expID, int numConts, int numNodes,
         int numCores) throws IOException {

      int memory = 1024;
      conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, memory);
      conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, numCores * memory);

      // Initialize resource Manager
      ResourceManager resourceManager = new ResourceManager();
      resourceManager.init(conf);

      AsyncDispatcher dispatcher = new InlineDispatcher();
      RMApplicationHistoryWriter writer = mock(RMApplicationHistoryWriter.class);
      ContainerAllocationExpirer expirer = new ContainerAllocationExpirer(dispatcher);

      ResourceScheduler scheduler = resourceManager.getResourceScheduler();
      RMContext rmContext = resourceManager.getRMContext();
      rmContext.setRMApplicationHistoryWriter(writer);
      ((RMContextImpl) rmContext).setContainerAllocationExpirer(expirer);
      ((RMContextImpl) rmContext).setSystemMetricsPublisher(mock(SystemMetricsPublisher.class));

      // Create the nodes
      ArrayList<RMNode> nodes = new ArrayList<RMNode>(numNodes);

      for (int i = 1; i <= numNodes; ++i) {
         RMNode node = MockNodes.newNodeInfo(1,
               Resources.createResource(memory * numCores, numCores), i, "host" + i);
         NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node);
         scheduler.handle(nodeEvent);
         nodes.add(node);
      }

      // Suspend scheduling thread
      if (scheduler instanceof HungarianScheduler) {
         ((HungarianScheduler) scheduler).suspendSchedule();
      } else if (scheduler instanceof CapacityScheduler) {
         ((CapacityScheduler) scheduler).suspendSchedulerThreads();
      }

      // Create the application
      int _appId = 1;
      int _appAttemptId = 1;
      ApplicationAttemptId appAttemptId = createAppAttemptId(_appId, _appAttemptId);

      createMockRMApp(appAttemptId, rmContext);

      AppAddedSchedulerEvent appEvent = new AppAddedSchedulerEvent(appAttemptId.getApplicationId(),
            "default", "hadoop");
      scheduler.handle(appEvent);
      AppAttemptAddedSchedulerEvent attemptEvent = new AppAttemptAddedSchedulerEvent(appAttemptId,
            false);
      scheduler.handle(attemptEvent);

      // Create resource requests
      int priority = 20;

      Random rand = new Random(0);
      HashMap<String, ResourceRequest> requests = new HashMap<String, ResourceRequest>();
      ResourceRequest nodeLocal;

      for (int i = 0; i < numConts * 3; ++i) {
         RMNode node = nodes.get(rand.nextInt(numNodes));
         if (requests.containsKey(node.getHostName())) {
            nodeLocal = requests.get(node.getHostName());
            nodeLocal.setNumContainers(nodeLocal.getNumContainers() + 1);
         } else {
            nodeLocal = createResourceRequest(memory, node.getHostName(), priority, 1);
            requests.put(node.getHostName(), nodeLocal);
         }
         nodeLocal.incrCountPreference(1 + (2 * rand.nextInt(3)));
      }

      ResourceRequest rackLocal = createResourceRequest(memory, nodes.get(0).getRackName(),
            priority, numConts);
      ResourceRequest any = createResourceRequest(memory, ResourceRequest.ANY, priority, numConts);

      List<ResourceRequest> ask = new ArrayList<ResourceRequest>(requests.values());
      ask.add(rackLocal);
      ask.add(any);
      scheduler.allocate(appAttemptId, ask, new ArrayList<ContainerId>(), null, null);

      // Make scheduling
      long startTime = System.currentTimeMillis();
      if (scheduler instanceof HungarianScheduler) {
         ((HungarianScheduler) scheduler).scheduleAsync();
      } else if (scheduler instanceof CapacityScheduler) {
         CapacityScheduler.schedule((CapacityScheduler) scheduler);
      } else if (scheduler instanceof FifoScheduler) {
         ((FifoScheduler) scheduler).scheduleAll();
      }

      long endTime = System.currentTimeMillis();

      LOG.info("Assignment done:\t" + scheduler.getClass().getSimpleName() + "\t" + expID + "\t"
            + numConts + "\t" + (numNodes * numCores) + "\t" + (endTime - startTime));

      resourceManager.stop();
      try {
         resourceManager.stopActiveServices();
      } catch (Exception e) {
         e.printStackTrace();
      }
      resourceManager.close();
   }

   private RMAppImpl createMockRMApp(ApplicationAttemptId attemptId, RMContext context) {
      RMAppImpl app = mock(RMAppImpl.class);
      when(app.getApplicationId()).thenReturn(attemptId.getApplicationId());
      RMAppAttemptImpl attempt = mock(RMAppAttemptImpl.class);
      when(attempt.getAppAttemptId()).thenReturn(attemptId);
      RMAppAttemptMetrics attemptMetric = mock(RMAppAttemptMetrics.class);
      when(attempt.getRMAppAttemptMetrics()).thenReturn(attemptMetric);
      when(app.getCurrentAppAttempt()).thenReturn(attempt);
      context.getRMApps().putIfAbsent(attemptId.getApplicationId(), app);
      return app;
   }

   private ApplicationAttemptId createAppAttemptId(int appId, int attemptId) {
      ApplicationId appIdImpl = ApplicationId.newInstance(0, appId);
      ApplicationAttemptId attId = ApplicationAttemptId.newInstance(appIdImpl, attemptId);
      return attId;
   }

   private ResourceRequest createResourceRequest(int memory, String host, int priority,
         int numContainers) {
      ResourceRequest request = recordFactory.newRecordInstance(ResourceRequest.class);
      request.setCapability(Resources.createResource(memory));
      request.setResourceName(host);
      request.setNumContainers(numContainers);
      Priority prio = recordFactory.newRecordInstance(Priority.class);
      prio.setPriority(priority);
      request.setPriority(prio);
      return request;
   }

}
