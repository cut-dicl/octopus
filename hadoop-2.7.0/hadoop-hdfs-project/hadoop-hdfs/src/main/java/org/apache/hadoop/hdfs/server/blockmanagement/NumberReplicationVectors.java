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
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.fs.ReplicationVector;

/**
 * A immutable object that stores the vectors of live, decommissioned, corrupt,
 * excess, and replicas-on-stale nodes
 */
public class NumberReplicationVectors {
  private ReplicationVector liveReplicas;
  private ReplicationVector decommissionedReplicas;
  private ReplicationVector corruptReplicas;
  private ReplicationVector excessReplicas;
  private ReplicationVector replicasOnStaleNodes;
  private ReplicationVector nonVolatileLiveReplicas;

  NumberReplicationVectors() {
    liveReplicas = new ReplicationVector(0l);
    decommissionedReplicas = new ReplicationVector(0l);
    corruptReplicas = new ReplicationVector(0l);
    excessReplicas = new ReplicationVector(0l);
    replicasOnStaleNodes = new ReplicationVector(0l);
    nonVolatileLiveReplicas = new ReplicationVector(0L);
  }

  NumberReplicationVectors(ReplicationVector live,
      ReplicationVector decommissioned, ReplicationVector corrupt,
      ReplicationVector excess, ReplicationVector stale,
      ReplicationVector nonVolatileLive) {
    this();
    initialize(live, decommissioned, corrupt, excess, stale, nonVolatileLive);
  }

  void initialize(ReplicationVector live, ReplicationVector decommissioned,
      ReplicationVector corrupt, ReplicationVector excess,
      ReplicationVector stale, ReplicationVector nonVolatileLive) {
    liveReplicas.setVectorEncoding(live.getVectorEncoding());
    decommissionedReplicas
        .setVectorEncoding(decommissioned.getVectorEncoding());
    corruptReplicas.setVectorEncoding(corrupt.getVectorEncoding());
    excessReplicas.setVectorEncoding(excess.getVectorEncoding());
    replicasOnStaleNodes.setVectorEncoding(stale.getVectorEncoding());
    nonVolatileLiveReplicas.setVectorEncoding(nonVolatileLive.getVectorEncoding());
  }

  public ReplicationVector liveReplicas() {
    return liveReplicas;
  }

  public ReplicationVector decommissionedReplicas() {
    return decommissionedReplicas;
  }

  public ReplicationVector corruptReplicas() {
    return corruptReplicas;
  }

  public ReplicationVector excessReplicas() {
    return excessReplicas;
  }

  /**
   * @return the number of replicas which are on stale nodes. This is not
   *         mutually exclusive with the other counts -- ie a replica may count
   *         as both "live" and "stale".
   */
  public ReplicationVector replicasOnStaleNodes() {
    return replicasOnStaleNodes;
  }
  
  /**
   * @return the number of live replicas on non-volatile storage
   */
  public ReplicationVector nonVolatileLiveReplicas() {
     return nonVolatileLiveReplicas;
  }
  
  public NumberReplicas toNumberReplicas() {
    return new NumberReplicas(liveReplicas.getTotalReplication(),
        decommissionedReplicas.getTotalReplication(),
        corruptReplicas.getTotalReplication(),
        excessReplicas.getTotalReplication(),
        replicasOnStaleNodes.getTotalReplication(),
        nonVolatileLiveReplicas.getTotalReplication());
  }
}
