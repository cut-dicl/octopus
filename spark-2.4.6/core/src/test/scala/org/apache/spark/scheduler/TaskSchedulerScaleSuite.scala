/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler

import java.nio.ByteBuffer

import scala.collection.mutable.HashMap
import scala.concurrent.duration._

import org.mockito.Matchers.{anyInt, anyObject, anyString, eq => meq}
import org.mockito.Mockito.{atLeast, atMost, never, spy, times, verify, when}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.Eventually
import org.scalatest.mockito.MockitoSugar

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config
import org.apache.spark.util.ManualClock
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.util.control.Breaks._

class FakeSchedulerBackendScale extends SchedulerBackend {
  def start() {}
  def stop() {}
  def reviveOffers() {}
  def defaultParallelism(): Int = 1
  def maxNumConcurrentTasks(): Int = 0
}

class TaskSchedulerScaleSuite extends SparkFunSuite with LocalSparkContext with BeforeAndAfterEach
    with Logging with MockitoSugar with Eventually {

  var failedTaskSetException: Option[Throwable] = None
  var failedTaskSetReason: String = null
  var failedTaskSet = false

  var blacklist: BlacklistTracker = null
  var taskScheduler: TaskSchedulerImpl = null
  var dagScheduler: DAGScheduler = null

  val stageToMockTaskSetBlacklist = new HashMap[Int, TaskSetBlacklist]()
  val stageToMockTaskSetManager = new HashMap[Int, TaskSetManager]()

  override def beforeEach(): Unit = {
    super.beforeEach()
    failedTaskSet = false
    failedTaskSetException = None
    failedTaskSetReason = null
    stageToMockTaskSetBlacklist.clear()
    stageToMockTaskSetManager.clear()
  }

  override def afterEach(): Unit = {
    if (taskScheduler != null) {
      taskScheduler.stop()
      taskScheduler = null
    }
    if (dagScheduler != null) {
      dagScheduler.stop()
      dagScheduler = null
    }
    super.afterEach()
  }

  def setupScheduler(confs: (String, String)*): TaskSchedulerImpl = {
    setupSchedulerWithMaster("local", confs: _*)
  }

  def setupSchedulerWithMaster(master: String, confs: (String, String)*): TaskSchedulerImpl = {
    val conf = new SparkConf().setMaster(master).setAppName("TaskSchedulerScaleSuite")
    confs.foreach { case (k, v) => conf.set(k, v) }
    sc = new SparkContext(conf)
    taskScheduler = new TaskSchedulerImpl(sc)
    setupHelper()
  }

  def setupSchedulerNoShuffle(confs: (String, String)*): TaskSchedulerImpl = {
    val conf = new SparkConf().setMaster("local").setAppName("TaskSchedulerScaleSuite")
    confs.foreach { case (k, v) => conf.set(k, v) }
    sc = new SparkContext(conf)
    taskScheduler = new TaskSchedulerImpl(sc){
      override def shuffleOffers(offers: IndexedSeq[WorkerOffer]): IndexedSeq[WorkerOffer] = {
        // Don't shuffle the offers around for this test.
        offers
      }
    }
    setupHelper()
  }

  def setupHelper(): TaskSchedulerImpl = {
    taskScheduler.initialize(new FakeSchedulerBackendScale)
    // Need to initialize a DAGScheduler for the taskScheduler to use for callbacks.
    dagScheduler = new DAGScheduler(sc, taskScheduler) {
      override def taskStarted(task: Task[_], taskInfo: TaskInfo): Unit = {}
      override def executorAdded(execId: String, host: String): Unit = {}
      override def taskSetFailed(
          taskSet: TaskSet,
          reason: String,
          exception: Option[Throwable]): Unit = {
        // Normally the DAGScheduler puts this in the event loop, which will eventually fail
        // dependent jobs
        failedTaskSet = true
        failedTaskSetReason = reason
        failedTaskSetException = exception
      }
    }
    taskScheduler
  }
  
  /**
   * Generate numHosts WorkerOffer. One executor per host. 
   */
  def generateWorkerOffers(numHosts: Int, numCoresPerHost: Int): IndexedSeq[WorkerOffer] = {
     return IndexedSeq.tabulate(numHosts)(
         h => new WorkerOffer("executor" + (h+1), "host" + (h+1), numCoresPerHost))
  }

  /**
   * Generate numTasks task locations. Three random host locations per task, one on each tier
   */
  def generateTaskLocations(numHosts: Int, numTasks: Int): ArrayBuffer[Seq[TaskLocation]] = {
    
    val hosts = List.tabulate(numHosts)(h => (h+1))
    val locations = ArrayBuffer[Seq[TaskLocation]]()
    
    for( a <- 1 to numTasks){
      val firstThree = Random.shuffle(hosts).take(3)
      locations += Seq(
          new PreferenceHostTaskLocation("host" + firstThree(0), 0), 
          new PreferenceHostTaskLocation("host" + firstThree(1), 3),
          new PreferenceHostTaskLocation("host" + firstThree(2), 5))
    }
    
    return locations
  }
  

  /**
   * To run only this scalability test, you can use this command from spark-2.4.6/core
   *   ../build/mvn -Dtest=none -DwildcardSuites=org.apache.spark.scheduler.TaskSchedulerScaleSuite test
   *
   * You might need to increase the available memory
   *   export MAVEN_OPTS="-Xmx24G -Xss1G -XX:MetaspaceSize=4G -XX:MaxMetaspaceSize=12G -XX:+CMSClassUnloadingEnabled"
   *
   * For more accurate timings, you should comment out line 764
   *   sched.dagScheduler.taskStarted(task, info)
   * in file
   *   spark-2.4.6/core/src/main/scala/org/apache/spark/scheduler/TaskSetManager.scala
   *   
   * You will find the results in the unit-tests.log
   *   grep SCALE target/unit-tests.log
   */ 
 
  test("Scalability test for scheduling tasks with tier awareness") {
  
    logInfo("SCALE\tnumSlots\tnumTasks\tuseHungarian\tenableTiers" + 
            "\tpruneResources\tpruneTasks\trepetition\tassignments\tduration")

    val numCpusPerNode = 2

    for (numNodes <- List.tabulate(3)(n => scala.math.pow(2, n+2).toInt)) {
      
      for (numTasks <- List.tabulate(3)(n => scala.math.pow(2, n+3).toInt)) {

        for (scenario <- Seq((false, false), (false, true), (true, false), (true, true))) {

          for (pruning <- Seq((false, false), (false, true), (true, false), (true, true))) {

          breakable {
          if (scenario._1 == false && (pruning._1 == true || pruning._2 == true)) {
            break
          }
          
          for (repetition <- List.tabulate(2)(n => n+1)) {

            this.beforeEach()
            
            Random.setSeed(41L)
            
            val taskScheduler = setupScheduler(
                "spark.scheduler.use.hungarian" -> scenario._1.toString,
                "spark.scheduler.enable.tiers" -> scenario._2.toString,
                "spark.scheduler.hungarian.prune.resources" -> pruning._1.toString,
                "spark.scheduler.hungarian.prune.tasks" -> pruning._2.toString,
                "spark.locality.wait" -> "0",
                "spark.network.timeout" -> "10000000",
                "spark.executor.heartbeatInterval" -> "10000000"
                )
        
            // Create offers 
            val offers = generateWorkerOffers(numNodes, numCpusPerNode)
            taskScheduler.resourceOffers(offers)
        
            // Create tasks with prefered locations
            val locations = generateTaskLocations(numNodes, numTasks)
            val taskSet = FakeTask.createTaskSet(locations.length, locations: _*)
        
            // Schedule tasks
            taskScheduler.submitTasks(taskSet)
            val t1 = System.nanoTime
            val taskDescriptions = taskScheduler.resourceOffers(offers).flatten
            val t2 = System.nanoTime
            val duration = (t2 - t1) / 1e6d
            
            logInfo(s"SCALE\t${numNodes * numCpusPerNode}\t${numTasks}" + 
                s"\t${scenario._1}\t${scenario._2}" + 
                s"\t${pruning._1}\t${pruning._2}" + 
                s"\t${repetition}\t${taskDescriptions.length}\t${duration}")
      
            this.afterEach()
          } // End for repetition
          } // End breakable
          } // End for pruning
        }
      }
    }
  }

}
