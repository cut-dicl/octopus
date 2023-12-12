/**
 * 
 */
package org.apache.hadoop.mapreduce.split;

import org.apache.hadoop.fs.StorageType;

/**
 * @author herodotos.herodotou
 */
public class SplitLocationDetails {

   private String rack;
   private String host;
   private StorageType type;

   public SplitLocationDetails(String rack, String host, StorageType type) {
      this.rack = rack;
      this.host = host;
      this.type = type;
   }

   public String getRack() {
      return rack;
   }

   public String getHost() {
      return host;
   }

   public StorageType getType() {
      return type;
   }
   
   @Override
   public String toString() {
      return rack + "/" + host + "[" + type + "]";
   }
}
