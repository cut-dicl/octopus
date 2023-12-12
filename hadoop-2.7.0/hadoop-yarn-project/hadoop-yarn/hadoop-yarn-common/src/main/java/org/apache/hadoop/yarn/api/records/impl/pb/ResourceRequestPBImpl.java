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

package org.apache.hadoop.yarn.api.records.impl.pb;


import java.util.List;
import java.util.NavigableMap;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.PreferencePair;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.proto.YarnProtos.PreferencePairProto;
import org.apache.hadoop.yarn.proto.YarnProtos.PriorityProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceRequestProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceRequestProtoOrBuilder;


@Private
@Unstable
public class ResourceRequestPBImpl extends  ResourceRequest {
  ResourceRequestProto proto = ResourceRequestProto.getDefaultInstance();
  ResourceRequestProto.Builder builder = null;
  boolean viaProto = false;
  
  private Priority priority = null;
  private Resource capability = null;
  private NavigableMap<Integer, Integer> preferences = null; //Added by EK
  
  public ResourceRequestPBImpl() {
    builder = ResourceRequestProto.newBuilder();
  }

  public ResourceRequestPBImpl(ResourceRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public ResourceRequestProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.priority != null) {
      builder.setPriority(convertToProtoFormat(this.priority));
    }
    if (this.capability != null) {
      builder.setCapability(convertToProtoFormat(this.capability));
    }
    builder.clearPreferences();
    if (this.preferences != null) {
       for (Entry<Integer, Integer> pair: preferences.entrySet()) {
          builder.addPreferences(convertToProtoFormat(
                PreferencePair.newInstance(pair.getKey(),pair.getValue())));
       }
    }
  }

  private PreferencePairPBImpl convertFromProtoFormat(PreferencePairProto p) {
	    return new PreferencePairPBImpl(p);
  }  

  private PreferencePairProto convertToProtoFormat(PreferencePair p) {
	    return ((PreferencePairPBImpl)p).getProto();
  }

  private void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ResourceRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public Priority getPriority() {
    ResourceRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.priority != null) {
      return this.priority;
    }
    if (!p.hasPriority()) {
      return null;
    }
    this.priority = convertFromProtoFormat(p.getPriority());
    return this.priority;
  }

  @Override
  public void setPriority(Priority priority) {
    maybeInitBuilder();
    if (priority == null) 
      builder.clearPriority();
    this.priority = priority;
  }
  @Override
  public String getResourceName() {
    ResourceRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasResourceName()) {
      return null;
    }
    return (p.getResourceName());
  }

  @Override
  public void setResourceName(String resourceName) {
    maybeInitBuilder();
    if (resourceName == null) {
      builder.clearResourceName();
      return;
    }
    builder.setResourceName((resourceName));
  }
  @Override
  public Resource getCapability() {
    ResourceRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.capability != null) {
      return this.capability;
    }
    if (!p.hasCapability()) {
      return null;
    }
    this.capability = convertFromProtoFormat(p.getCapability());
    return this.capability;
  }

  @Override
  public void setCapability(Resource capability) {
    maybeInitBuilder();
    if (capability == null) 
      builder.clearCapability();
    this.capability = capability;
  }
  @Override
  public synchronized int getNumContainers() {
    ResourceRequestProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getNumContainers());
  }

  @Override
  public synchronized void setNumContainers(int numContainers) {
    maybeInitBuilder();
    builder.setNumContainers((numContainers));
  }
  
  @Override
  public boolean getRelaxLocality() {
    ResourceRequestProtoOrBuilder p = viaProto ? proto : builder;
    return p.getRelaxLocality();
  }

  @Override
  public void setRelaxLocality(boolean relaxLocality) {
    maybeInitBuilder();
    builder.setRelaxLocality(relaxLocality);
  }
  
  private PriorityPBImpl convertFromProtoFormat(PriorityProto p) {
    return new PriorityPBImpl(p);
  }

  private PriorityProto convertToProtoFormat(Priority t) {
    return ((PriorityPBImpl)t).getProto();
  }

  private ResourcePBImpl convertFromProtoFormat(ResourceProto p) {
    return new ResourcePBImpl(p);
  }

  private ResourceProto convertToProtoFormat(Resource t) {
    return ((ResourcePBImpl)t).getProto();
  }
  
  @Override
  public String toString() {
    return "{Priority: " + getPriority() + ", Capability: " + getCapability()
        + ", # Containers: " + getNumContainers()
        + ", Location: " + getResourceName()
        + ", Relax Locality: " + getRelaxLocality() 
        + ", Preferences: " + getPreferences() + "}";
  }

  @Override
  public String getNodeLabelExpression() {
    ResourceRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasNodeLabelExpression()) {
      return null;
    }
    return (p.getNodeLabelExpression());
  }

  @Override
  public void setNodeLabelExpression(String nodeLabelExpression) {
    maybeInitBuilder();
    if (nodeLabelExpression == null) {
      builder.clearNodeLabelExpression();
      return;
    }
    builder.setNodeLabelExpression(nodeLabelExpression);
  }

////////////////////////////////////////////////////////////////////
// Added by EK for Task Scheduling over Tiered Storage Systems work
////////////////////////////////////////////////////////////////////
  @Override
  public synchronized void setPreferences(NavigableMap<Integer, Integer> p) {
	  maybeInitBuilder();
	  if (p == null)
		  builder.clearPreferences();
	  this.preferences = p;
  }
  
  @Override
  public synchronized NavigableMap<Integer, Integer> getPreferences() {
     if (this.preferences != null) {
       return this.preferences;
     }
     
     this.preferences = new TreeMap<Integer, Integer>();
     ResourceRequestProtoOrBuilder p = viaProto ? proto : builder;
     List<PreferencePairProto> pProtos = p.getPreferencesList();

     if (pProtos != null) {
        PreferencePairPBImpl pair;
        for (PreferencePairProto r : pProtos) {
           pair = convertFromProtoFormat(r);
           this.preferences.put(pair.getOrderValue(), pair.getCount());
        }
     }

     return this.preferences;
  }
    
  @Override
  public synchronized int getPreferenceCount(int orderType) {
     NavigableMap<Integer, Integer> prefs = getPreferences();
     if (prefs.containsKey(orderType))
        return prefs.get(orderType);
     else
        return 0;
  }
  
  @Override
  public synchronized void incrCountPreference(int orderType) {
     NavigableMap<Integer, Integer> prefs = getPreferences();
     if (prefs.containsKey(orderType))
        prefs.put(orderType, prefs.get(orderType) + 1);
     else
        prefs.put(orderType, 1);
  }
  
  @Override
  public synchronized void decrCountPreference(int orderType) {
     NavigableMap<Integer, Integer> prefs = getPreferences();
     if (prefs.containsKey(orderType)) {
        int count = prefs.get(orderType) - 1;
        if (count > 0)
           prefs.put(orderType, count);
        else
           prefs.remove(orderType);
     }
  }
  
}