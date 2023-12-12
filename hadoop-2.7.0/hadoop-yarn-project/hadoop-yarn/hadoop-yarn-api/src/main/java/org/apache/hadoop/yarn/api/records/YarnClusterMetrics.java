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

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p><code>YarnClusterMetrics</code> represents cluster metrics.</p>
 * 
 * <p>Currently only number of <code>NodeManager</code>s is provided.</p>
 */
@Public
@Stable
public abstract class YarnClusterMetrics {
  
  @Private
  @Unstable
  public static YarnClusterMetrics newInstance(int numNodeManagers) {
    YarnClusterMetrics metrics = Records.newRecord(YarnClusterMetrics.class);
    metrics.setNumNodeManagers(numNodeManagers);
    return metrics;
  }

  //---------------------------------------
  //Added for Octopus Prefetching by Elena
  //---------------------------------------
  @Private
  @Unstable
  public static YarnClusterMetrics newInstance(int numNodeManagers, int allocatedMemory, int availableMemory, int reservedMemory, int allocatedCores, int availableCores, int reservedCores, int appsRunning, int appsPending) {
    YarnClusterMetrics metrics = Records.newRecord(YarnClusterMetrics.class);
    metrics.setNumNodeManagers(numNodeManagers);
    metrics.setNumAllocatedMemory(allocatedMemory);
    metrics.setNumAvailableMemory(availableMemory);
    metrics.setNumReservedMemory(reservedMemory);
    metrics.setNumAllocatedCores(allocatedCores);
    metrics.setNumAvailableCores(availableCores);
    metrics.setNumReservedCores(reservedCores);
    metrics.setNumAppsRunning(appsRunning);
    metrics.setNumAppsPending(appsPending);
    return metrics;
  }
  //---------------------------------------
  /**
   * Get the number of <code>NodeManager</code>s in the cluster.
   * @return number of <code>NodeManager</code>s in the cluster
   */
  @Public
  @Stable
  public abstract int getNumNodeManagers();

  @Private
  @Unstable
  public abstract void setNumNodeManagers(int numNodeManagers);

  
  //---------------------------------------
  //Added for Octopus Prefetching by Elena
  //---------------------------------------
  
/**
 * Get the number of <code>allocatedMemory</code>s in the cluster.
 * @return number of <code>allocatedMemory</code>s in the cluster
 */
@Public
@Stable
public abstract int getNumAllocatedMemory();

@Private
@Unstable
public abstract void setNumAllocatedMemory(int allocatedMemory);

/**
* Get the number of <code>availableMemory</code>s in the cluster.
* @return number of <code>availableMemory</code>s in the cluster
*/
@Public
@Stable
public abstract int getNumAvailableMemory();

@Private
@Unstable
public abstract void setNumAvailableMemory(int availableMemory);


/**
* Get the number of <code>reservedMemory</code>s in the cluster.
* @return number of <code>reservedMemory</code>s in the cluster
*/
@Public
@Stable
public abstract int getNumReservedMemory();

@Private
@Unstable
public abstract void setNumReservedMemory(int reservedMemory);


/**
* Get the number of <code>allocatedCores</code>s in the cluster.
* @return number of <code>allocatedCores</code>s in the cluster
*/
@Public
@Stable
public abstract int getNumAllocatedCores();

@Private
@Unstable
public abstract void setNumAllocatedCores(int allocatedCores);

/**
* Get the number of <code>availableCores</code>s in the cluster.
* @return number of <code>availableCores</code>s in the cluster
*/
@Public
@Stable
public abstract int getNumAvailableCores();

@Private
@Unstable
public abstract void setNumAvailableCores(int availableCores);

/**
* Get the number of <code>reservedCores</code>s in the cluster.
* @return number of <code>reservedCores</code>s in the cluster
*/
@Public
@Stable
public abstract int getNumReservedCores();

@Private
@Unstable
public abstract void setNumReservedCores(int reservedCores);

/**
* Get the number of <code>appsRunning</code>s in the cluster.
* @return number of <code>appsRunning</code>s in the cluster
*/
@Public
@Stable
public abstract int getNumAppsRunning();

@Private
@Unstable
public abstract void setNumAppsRunning(int appsRunning);

/**
* Get the number of <code>appsPending</code>s in the cluster.
* @return number of <code>appsPending</code>s in the cluster
*/
@Public
@Stable
public abstract int getNumAppsPending();

@Private
@Unstable
public abstract void setNumAppsPending(int appsPending);

//---------------------------------------


}
