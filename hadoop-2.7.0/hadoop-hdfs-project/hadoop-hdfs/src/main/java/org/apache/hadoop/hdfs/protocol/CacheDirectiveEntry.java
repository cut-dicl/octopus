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

import java.util.EnumMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.StorageType;

/**
 * Describes a path-based cache directive entry.
 */
@InterfaceStability.Evolving
@InterfaceAudience.Public
public class CacheDirectiveEntry {
  private final CacheDirectiveInfo info;
  private final EnumMap<StorageType, CacheDirectiveStats> statsPerType;
  
  public CacheDirectiveEntry(CacheDirectiveInfo info, EnumMap<StorageType, CacheDirectiveStats> statsPerType) {
    this.info = info;
    this.statsPerType = statsPerType;
  }

  public CacheDirectiveInfo getInfo() {
    return info;
  }

  public EnumMap<StorageType, CacheDirectiveStats> getStats() {
    return statsPerType;
  }
};