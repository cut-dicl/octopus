/**
 * Describes a pair of preferences map for Task Scheduling over Tiered Storage Systems work
 * 
 * @author elena.kakoulli
 */

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.yarn.util.Records;

/**
 * PreferencePair models the order value of a storage type with its count (based on container request)
 * 
 * @see ResourceRequestPBImpl
 * @see PreferencePairPBImpl
 */
@Public
@Stable
public abstract class PreferencePair {

  @Public
  @Stable
  public static PreferencePair newInstance(int orderValue, int count) {
    PreferencePair preference = Records.newRecord(PreferencePair.class);
    preference.setOrderValue(orderValue);
    preference.setCount(count);
    return preference;
  }

  /**
   * Get <em>order value</em> of the storage type.
   * 
   * @return <em>order value</em> of the storage type
   */
  @Public
  @Stable
  public abstract int getOrderValue();
  
  /**
   * Set <em>order value</em> of the storage type.
   * 
   * @param orderValue <em>the order value</em> of the storage type
   */
  @Public
  @Stable
  public abstract void setOrderValue(int orderValue);


  /**
   * Get <em>count of the order value</em> of the storage type.
   *   
   * @return <em>count the order value</em> of the storage type.
   */
  @Public
  @Evolving
  public abstract int getCount();
  
  /**
   * Set <em>count of the order value</em> of the storage type.
   *    
   * @param count <em>the count of the order value</em> of the storage type
   */
  @Public
  @Evolving
  public abstract void setCount(int count);

}
