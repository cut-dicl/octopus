/**
 * 
 * @see PreferencePair abstract class
 * 
 * Describes the use of a pair in preferences map for Task Scheduling over Tiered Storage Systems work
 * 
 * @author elena.kakoulli
 */

package org.apache.hadoop.yarn.api.records.impl.pb;


import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.PreferencePair;
import org.apache.hadoop.yarn.proto.YarnProtos.PreferencePairProto;
import org.apache.hadoop.yarn.proto.YarnProtos.PreferencePairProtoOrBuilder;

@Private
@Unstable
public class PreferencePairPBImpl extends PreferencePair {
	PreferencePairProto proto = PreferencePairProto.getDefaultInstance();
	PreferencePairProto.Builder builder = null;
	boolean viaProto = false;
  
  public PreferencePairPBImpl() {
    builder = PreferencePairProto.newBuilder();
  }

  public PreferencePairPBImpl(PreferencePairProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public PreferencePairProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = PreferencePairProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  @Override
  public int getOrderValue() {
	PreferencePairProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getOrderValue());
  }

  @Override
  public void setOrderValue(int orderValue) {
    maybeInitBuilder();
    builder.setOrderValue((orderValue));
  }

  @Override
  public int getCount() {
	PreferencePairProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getCount());
  }

  @Override
  public void setCount(int count) {
    maybeInitBuilder();
    builder.setCount((count));
  }
   
}  
