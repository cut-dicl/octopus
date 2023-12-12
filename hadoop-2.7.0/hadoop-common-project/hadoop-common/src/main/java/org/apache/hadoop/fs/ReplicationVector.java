package org.apache.hadoop.fs;

/**
 * The replication vector specifies the number of replicas for each storage
 * type. For example, <TOTAL, DISK, SSD, ARCHIVE, RAM_DISK>=<3, 2, 1, 0, 0>
 * specifies 2 replicas for disk and 1 replica for SSD for a total of 3
 * replicas.
 * 
 * The replication vector is encoded into a 64-bit long, where the first 48
 * bits store the N replication factors (in order of the StorageType definition)
 * and the last 16 bits store the total replication.
 * 
 * For backwards compatibility, the total replication does not have to equal the
 * sum of the individual replication factors, but it must always be at least as
 * much. In this case, the storage type for some replicas is not explicitly
 * defined and the placement policies will decide how to place the extra block
 * replicas. For example, the replication vector <3, 1, 1, 0, 0> specifies 1
 * replica for disk, 1 for SSD, and 1 is unspecified.
 * 
 * @author herodotos.herodotou
 *
 */
public class ReplicationVector {

  // Initialize masks and offsets for each storage type
  private static long[] Masks = new long[StorageType.COUNT + 1];
  private static int[] Offsets = new int[StorageType.COUNT + 1];

  private static int TOTALS_OFFSET = 0;
  private static int TOTALS_LENGTH = 16;

  private static void init() {
    // For the total count
    Offsets[TOTALS_OFFSET] = 0;
    Masks[TOTALS_OFFSET] = ((-1L) >>> (Long.SIZE - TOTALS_LENGTH));

    // For the individual factors
    int length = (Long.SIZE - TOTALS_LENGTH) / StorageType.COUNT;
    long max = ((-1L) >>> (Long.SIZE - length));

    for (StorageType type : StorageType.values()) {
      int ordinal = type.ordinal();
      Offsets[ordinal + 1] = (ordinal * length) + TOTALS_LENGTH;
      Masks[ordinal + 1] = max << Offsets[ordinal + 1];
    }
  }

  static {
    ReplicationVector.init();
  }

  // Bit Format: |_48/N_| bits for each factor of the N storage types
  // and 16 bits for the total count
  private long vectorEncoding;

  /**
   * Initialize using an existing vector encoding
   * 
   * @param vectorEncoding
   */
  public ReplicationVector(long vectorEncoding) {
    setVectorEncoding(vectorEncoding);
  }

  /**
   * Initialize using an existing replication vector
   * 
   * @param vectorEncoding
   */
  public ReplicationVector(ReplicationVector replVector) {
    this.vectorEncoding = replVector.getVectorEncoding();
  }

  /**
   * Initialize by counting each replication type in the provided list
   * 
   * @param types
   */
  public ReplicationVector(Iterable<StorageType> types) {
    this.vectorEncoding = 0l;
    for (StorageType type : types) {
      incrReplication(type, (short) 1);
    }
  }

  /**
   * Set the replication factor for a specific storage type
   * 
   * @param type
   * @param replication
   */
  public void setReplication(StorageType type, short replication) {

    short oldRepl = getReplication(type);
    short oldTotal = getTotalReplication();
    short newRepl = (replication < 0) ? 0 : replication;
    short newTotal = (short) (oldTotal - oldRepl + newRepl);

    // Set the new replication for the specific type
    if (type != null) {
      int offset = type.ordinal() + 1;
      vectorEncoding = (vectorEncoding & ~Masks[offset])
          | (((long) newRepl) << Offsets[offset]);
    }

    // Update the total count
    vectorEncoding = (vectorEncoding & ~Masks[TOTALS_OFFSET])
        | (((long) newTotal) << Offsets[TOTALS_OFFSET]);
  }

  /**
   * Increment the replication factor for a specific storage type by the desired
   * amount
   * 
   * @param type
   * @param toAdd
   */
  public void incrReplication(StorageType type, short toAdd) {
    setReplication(type, (short) (getReplication(type) + toAdd));
  }

  /**
   * Decrement the replication factor for a specific storage type by the desired
   * amount. If the amount to subtract is greater than the current factor, then
   * the factor is set to zero.
   * 
   * @param type
   * @param toSubtract
   */
  public void decrReplication(StorageType type, short toSubtract) {
    setReplication(type, (short) (getReplication(type) - toSubtract));
  }

  /**
   * Add the replication factor of each type (including the unspecified) of the
   * provided vector to the current vector
   * 
   * @param toAdd
   */
  public void addReplicationVector(ReplicationVector toAdd) {
    for (StorageType type : StorageType.values()) {
      incrReplication(type, toAdd.getReplication(type));
    }
    incrReplication(null, toAdd.getReplication(null));
  }

  /**
   * Subtract the replication factor of each type (including the unspecified) of
   * the provided vector to the current vector. If any factor to subtract is
   * greater than the current one, then the current one is set to zero.
   * 
   * @param toSubtract
   */
  public void subtractReplicationVector(ReplicationVector toSubtract) {
    for (StorageType type : StorageType.values()) {
      decrReplication(type, toSubtract.getReplication(type));
    }
    decrReplication(null, toSubtract.getReplication(null));
  }

  /**
   * Remove the specified number of replicas. The removal policy works in three
   * stages: 1. Remove unspecified replicas 2. Remove replicas from type where
   * replication > 1 in round robin 3. Remove replicas from top to bottom
   * 
   * @param toRemove
   */
  public void removeReplicas(short toRemove) {
    short total = getTotalReplication();
    short target = (short) (total - toRemove);

    if (target <= 0) {
      // Remove all of them
      vectorEncoding = 0l;
      return;
    }

    // 1. Remove unspecified replicas
    decrReplication(null, toRemove);
    short remaining = getTotalReplication();
    if (target == remaining) {
      return;
    }

    // 2. Remove replicas from type where replication > 1 in round robin
    StorageType types[] = StorageType.values();
    int index = 0;
    while (getMaxReplicationFactor() > 1 && target < remaining) {
      if (getReplication(types[index]) > 1) {
        decrReplication(types[index], (short) 1);
        remaining = getTotalReplication();
      }
      index = (index + 1) % types.length;
    }

    if (target == remaining) {
      return;
    }

    // 3. Remove replicas from top to bottom
    for (StorageType type : StorageType.valuesOrdered()) {
      decrReplication(type, (short) 1);
      remaining = getTotalReplication();
      if (target == remaining) {
        return;
      }
    }
  }

  /**
   * Get the replication factor for a specific storage type
   * 
   * @param type
   * @return
   */
  public short getReplication(StorageType type) {
    return GetReplication(type, vectorEncoding);
  }

  /**
   * Get the sum of the individual replication factors (excluding the
   * unspecified ones)
   * 
   * @return
   */
  public short getReplicationSum() {
    return GetReplicationSum(vectorEncoding);
  }

  /**
   * Returns the number of non-volatile replicas (it includes unspecified ones)
   * 
   * @return
   */
  public int getNonVolatileReplicationCount() {
     return GetNonVolatileReplicationCount(vectorEncoding);
  }
  
  /**
   * Get the total number of replicas
   * 
   * @return
   */
  public short getTotalReplication() {
    return (short) ((vectorEncoding & Masks[TOTALS_OFFSET]) >>> Offsets[TOTALS_OFFSET]);
  }

  /**
   * Get the maximum replication factor among the different storage types (NOT
   * counting the unspecified ones)
   * 
   * @return
   */
  public short getMaxReplicationFactor() {
    short curr, max = 0;

    for (StorageType type : StorageType.values()) {
      curr = GetReplication(type, vectorEncoding);
      if (curr > max) {
        max = curr;
      }
    }

    return max;
  }

   /**
    * Get the number of distinct replication factors among the different storage
    * types (NOT counting the unspecified ones).
    * 
    * Example: For RV <0, 1, 1, 0, 2, 0>, this method returns 3
    * 
    * @return
    */
   public short getDistinctReplFactors() {
      short curr, count = 0;

      for (StorageType type : StorageType.values()) {
         curr = GetReplication(type, vectorEncoding);
         if (curr > 0) {
            ++count;
         }
      }

      return count;
   }

  /**
   * Get the encoding of the replication vector as a long
   * 
   * @return
   */
  public long getVectorEncoding() {
    return vectorEncoding;
  }

  /**
   * Set the encoding of the replication vector
   * 
   * @param vectorEncoding
   */
  public void setVectorEncoding(long vectorEncoding) {
    ValidateVectorEncoding(vectorEncoding);
    this.vectorEncoding = vectorEncoding;
  }

  /**
   * Set this replication vector equal to the provided one
   * 
   * @param replVector
   */
  public void setReplicationVector(ReplicationVector replVector) {
    this.vectorEncoding = replVector.getVectorEncoding();
  }

  /**
   * Returns true if the replication factors for ALL storage types of the
   * current vector are greater than or equal to the corresponding types of the
   * provided vector
   * 
   * @param replVector
   * @return
   */
  public boolean isAllGreaterOrEqual(ReplicationVector replVector) {
    for (StorageType type : StorageType.values()) {
      if (getReplication(type) < replVector.getReplication(type)) {
        return false;
      }
    }

    if (getReplication(null) < replVector.getReplication(null)) {
      return false;
    }

    return true;
  }

  /**
   * Returns true if the replication factor for ANY storage type of the current
   * vector is strictly greater than the corresponding type of the provided
   * vector
   * 
   * @param replVector
   * @return
   */
  public boolean isAnyGreater(ReplicationVector replVector) {
    for (StorageType type : StorageType.values()) {
      if (getReplication(type) > replVector.getReplication(type)) {
        return true;
      }
    }

    if (getReplication(null) > replVector.getReplication(null)) {
      return true;
    }

    return false;
  }

  @Override
  public int hashCode() {
    return (int) (vectorEncoding ^ (vectorEncoding >>> 32));
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    ReplicationVector other = (ReplicationVector) obj;
    if (vectorEncoding != other.vectorEncoding)
      return false;
    return true;
  }

  @Override
  public String toString() {
    return StringifyReplVector(this.vectorEncoding);
  }

  /**
   * Add the two replication vectors together and return a new one
   * 
   * @param rv1
   * @param rv2
   * @return
   */
  public static ReplicationVector AddReplicationVectors(ReplicationVector rv1,
      ReplicationVector rv2) {
    ReplicationVector rvTotal = new ReplicationVector(rv1);
    rvTotal.addReplicationVector(rv2);
    return rvTotal;
  }

  /**
   * Subtract the second replication vector from the first one and return a new
   * one.
   * 
   * If rv2 contains a factor that is greater than the corresponding factor in
   * rv1, then some other factor will decrease by the extra amount based on the
   * policy of {@link ReplicationVector#removeReplicas(short)}. The goal is for
   * the resulting vector to have the same total as (rv1.total - rv2.total)
   * 
   * @param rv1
   * @param rv2
   * @return
   */
  public static ReplicationVector SubtractReplicationVectors(
      ReplicationVector rv1, ReplicationVector rv2) {
    ReplicationVector rvTotal = new ReplicationVector(rv1);
    rvTotal.subtractReplicationVector(rv2);

    int rv1Total = rv1.getTotalReplication();
    int rv2Total = rv2.getTotalReplication();
    int rvTotalTotal = rvTotal.getTotalReplication();

    if (rvTotalTotal != rv1Total - rv2Total) {
      rvTotal.removeReplicas((short) (rvTotalTotal - (rv1Total - rv2Total)));
    }

    return rvTotal;
  }

  /**
   * Get the replication factor for a specific type from the vector
   * 
   * If type is null, get the unspecified replication
   * 
   * @param type
   * @param replVector
   * @return
   */
  public static short GetReplication(StorageType type, long replVector) {
    if (type != null) {
      // Get the specific replication
      int offset = type.ordinal() + 1;
      return (short) ((replVector & Masks[offset]) >>> Offsets[offset]);
    } else {
      // Get the unspecified replication
      return (short) (GetTotalReplication(replVector) - GetReplicationSum(replVector));
    }

  }

  /**
   * Get the total number of replicas
   * 
   * @return
   */
  public static short GetTotalReplication(long replVector) {
    return (short) ((replVector & Masks[TOTALS_OFFSET]) >>> Offsets[TOTALS_OFFSET]);
  }

  /**
   * Get the sum of the individual replication factors (excluding the
   * unspecified ones)
   * 
   * @return
   */
  public static short GetReplicationSum(long replVector) {
    short sum = 0;

    for (StorageType type : StorageType.values()) {
      sum += GetReplication(type, replVector);
    }

    return sum;
  }

  /**
   * Returns the number of non-volatile replicas (it includes unspecified ones)
   * 
   * @param replVector
   * @return
   */
  public static int GetNonVolatileReplicationCount(long replVector) {
     short volSum = 0;

     for (StorageType type : StorageType.values()) {
        if (type.isVolatile())
           volSum += GetReplication(type, replVector);
     }

     return GetTotalReplication(replVector) - volSum;
  }

  /**
   * Returns the highest storage type in the replication vector
   * 
   * @param replVector
   * @return
   */
  public static StorageType getHighestStorageType(long replVector) {
     for (StorageType type : StorageType.valuesOrdered()) {
        if (GetReplication(type, replVector) > 0)
           return type;
     }
     
     return StorageType.DEFAULT;
  }

  /**
   * Returns the lowest storage type in the replication vector
   * 
   * @param replVector
   * @return
   */
  public static StorageType getLowestStorageType(long replVector) {
     StorageType[] types = StorageType.valuesOrdered();
     for (int i = types.length - 1; i >= 0; i--) {
        if (GetReplication(types[i], replVector) > 0)
           return types[i];
     }
     
     return StorageType.DEFAULT;
  }

  /**
   * Convert a replication vector encoding into a string representation.
   * It only includes the types for which the replication is not zero.
   * 
   * @param replVector
   * @return
   */
  public static String StringifyReplVector(long replVector) {
     return StringifyReplVector(replVector, false);
  }
  
  /**
   * Convert a replication vector encoding into a string representation.
   * It only includes the types for which the replication is not zero.
   * 
   * @param replVector
   * @param useAbbr
   * @return
   */
  public static String StringifyReplVector(long replVector, boolean useAbbr) {
    StringBuilder sb = new StringBuilder("[");
    boolean found = false;
    short r = 0;

    for (StorageType type : StorageType.valuesOrdered()) {
       r = GetReplication(type, replVector);
       if (r > 0) {
          sb.append((useAbbr ? type.getAbbreviation() : type));
          sb.append('=');
          sb.append(r);
          sb.append(',');
          found = true;
       }
    }

    r = (short) (GetTotalReplication(replVector) - GetReplicationSum(replVector));
    if (r > 0) {
       sb.append((useAbbr ? "U" : "UNSPEC="));
       sb.append(r);
    } else if (found) {
       sb.deleteCharAt(sb.length() - 1);
    } else {
       sb.append(0);
    }
    
    sb.append(']');

    return sb.toString();
  }

  /**
   * Parse the string version of the replication vector into a long encoding.
   * Format:
   * [DISK=<repl>,SSD=<repl>,RAM_DISK=<repl>,ARCHIVE=<repl>,UNSPEC=<repl>] All
   * parts are optional and may appear in any order.
   * 
   * A single number N is a shorthand notation for UNSCEC=N
   * 
   * @param replVector
   * @return
   */
  public static long ParseReplicationVector(String replVector) {

    // Remove brackets
    if (replVector.startsWith("[") && replVector.endsWith("]")) {
      replVector = replVector.substring(1, replVector.length() - 1);
    }

    if (replVector.isEmpty())
       return 0;
    
    ReplicationVector rv = new ReplicationVector(0l);

    try {
      // Parse the key-value pairs of the vector
      String[] pairs = replVector.split(",");
      for (String pair : pairs) {
        int i = pair.indexOf('=');
        if (i >= 0) {
          // Add the replication factor for the corresponding type
          String type = pair.substring(0, i).trim();
          if (type.equalsIgnoreCase("UNSPEC") || type.equalsIgnoreCase("U")) {
            rv.setReplication(null, ParseValidateReplication(pair.substring(i + 1)));
          } else {
            rv.setReplication(StorageType.parseStorageType(type),
                  ParseValidateReplication(pair.substring(i + 1)));
          }
        } else {
          // The replication vector is just a number
          rv.setReplication(null, ParseValidateReplication(pair));
        }
      }
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Unable to parse replication vector "
          + replVector, e);
    }

    return rv.getVectorEncoding();
  }

  /**
   * Validate the format of the vector encoding
   * 
   * @param vectorEncoding
   */
  private static void ValidateVectorEncoding(long vectorEncoding) {
    short sum = GetReplicationSum(vectorEncoding);
    short total = GetTotalReplication(vectorEncoding);

    if (total < 0 || sum < 0) {
      throw new IllegalArgumentException("Invalid replication vector:"
          + " The total number of replicas cannot be negative.");
    }

    if (sum > total) {
      throw new IllegalArgumentException("Invalid replication vector:"
          + " The sum of the replication factors " + sum
          + " cannot be greater than the total " + total);
    }
  }

  /**
   * Parse and validate a string into a replication factor
   * 
   * @param replication
   * @return
   */
  private static short ParseValidateReplication(String replication) {
     short repl = Short.parseShort(replication);
     
     if (repl < 0) {
         throw new IllegalArgumentException(
               "A replication factor cannot be negative: " + replication);
     }
     
     return repl;
  }
  
}
