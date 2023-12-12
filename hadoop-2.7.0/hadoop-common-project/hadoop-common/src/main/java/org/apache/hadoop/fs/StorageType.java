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

package org.apache.hadoop.fs;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.StringUtils;

/**
 * Defines the types of supported storage media. The default storage
 * medium is assumed to be DISK.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public enum StorageType {
  DISK(1 << StorageFlags.DRIVE_BASED_BIT),
  SSD(1 << StorageFlags.DRIVE_BASED_BIT),
  ARCHIVE(1 << StorageFlags.DRIVE_BASED_BIT),
  RAM_DISK((1 << StorageFlags.TRANSIENT_BIT)
           + (1 << StorageFlags.DRIVE_BASED_BIT)),
  MEMORY(1 << StorageFlags.VOLATILE_BIT),
  DISK_H(1 << StorageFlags.DRIVE_BASED_BIT),
  SSD_H(1 << StorageFlags.DRIVE_BASED_BIT),
  REMOTE(1 << StorageFlags.REMOTE_BIT);

  public static final int COUNT = 8;
  private final int flags;

  private static class StorageFlags {
    static final int TRANSIENT_BIT = 0;
    static final int VOLATILE_BIT = 1;
    static final int DRIVE_BASED_BIT = 2;
    static final int REMOTE_BIT = 3;
  }

  public static final StorageType DEFAULT = DISK;

  public static final StorageType[] EMPTY_ARRAY = {};

  private static final StorageType[] VALUES = values();

  private static final StorageType[] ORDERED_VALUES = { StorageType.MEMORY,
         StorageType.RAM_DISK, StorageType.SSD_H, StorageType.SSD,
         StorageType.DISK_H, StorageType.DISK, StorageType.ARCHIVE,
         StorageType.REMOTE };

   private static final StorageType[] CUSTOM_ORDER = { StorageType.DISK,
         StorageType.DISK_H, StorageType.SSD, StorageType.SSD_H,
         StorageType.MEMORY, StorageType.RAM_DISK, StorageType.ARCHIVE,
         StorageType.REMOTE };

  private static final EnumMap<StorageType, Integer> ORDERED_MAP = 
        new EnumMap<StorageType, Integer>(StorageType.class);
  
  private static final HashMap<String, StorageType> STRINGS_MAP;
  
  private static final EnumMap<StorageType, String> ABBREV_MAP = 
        new EnumMap<StorageType, String>(StorageType.class);
  
  static {
     // Mappings from StorageType to order in ordered array
     ORDERED_MAP.put(StorageType.MEMORY, 0);
     ORDERED_MAP.put(StorageType.RAM_DISK, 1);
     ORDERED_MAP.put(StorageType.SSD_H, 2);
     ORDERED_MAP.put(StorageType.SSD, 3);
     ORDERED_MAP.put(StorageType.DISK_H, 4);
     ORDERED_MAP.put(StorageType.DISK, 5);
     ORDERED_MAP.put(StorageType.ARCHIVE, 6);
     ORDERED_MAP.put(StorageType.REMOTE, 7);

     // Mappings from string to StorageType
     STRINGS_MAP = new HashMap<String, StorageType>(2 * COUNT + 2);
     STRINGS_MAP.put("D", StorageType.DISK);
     STRINGS_MAP.put("DISK", StorageType.DISK);
     STRINGS_MAP.put("S", StorageType.SSD);
     STRINGS_MAP.put("SSD", StorageType.SSD);
     STRINGS_MAP.put("A", StorageType.ARCHIVE);
     STRINGS_MAP.put("ARCHIVE", StorageType.ARCHIVE);
     STRINGS_MAP.put("RD", StorageType.RAM_DISK);
     STRINGS_MAP.put("RAM_DISK", StorageType.RAM_DISK);
     STRINGS_MAP.put("M", StorageType.MEMORY);
     STRINGS_MAP.put("MEMORY", StorageType.MEMORY);
     STRINGS_MAP.put("DH", StorageType.DISK_H);
     STRINGS_MAP.put("DISK_H", StorageType.DISK_H);
     STRINGS_MAP.put("SH", StorageType.SSD_H);
     STRINGS_MAP.put("SSD_H", StorageType.SSD_H);
     STRINGS_MAP.put("R", StorageType.REMOTE);
     STRINGS_MAP.put("REMOTE", StorageType.REMOTE);
     
     // Mappings from StorageType to string abbreviations
     ABBREV_MAP.put(StorageType.MEMORY, "M");
     ABBREV_MAP.put(StorageType.RAM_DISK, "RD");
     ABBREV_MAP.put(StorageType.SSD_H, "SH");
     ABBREV_MAP.put(StorageType.SSD, "S");
     ABBREV_MAP.put(StorageType.DISK_H, "DH");
     ABBREV_MAP.put(StorageType.DISK, "D");
     ABBREV_MAP.put(StorageType.ARCHIVE, "A");
     ABBREV_MAP.put(StorageType.REMOTE, "R");
  }

  StorageType(int flags) {
    this.flags = flags;
  }

  public boolean isTransient() {
    return ((flags >>> StorageFlags.TRANSIENT_BIT) & 1) != 0;
  }

  public boolean supportTypeQuota() {
    return ((flags >>> StorageFlags.TRANSIENT_BIT) & 1) == 0;
  }

  public boolean isMovable() {
    return ((flags >>> StorageFlags.TRANSIENT_BIT) & 1) == 0
            && ((flags >>> StorageFlags.REMOTE_BIT) & 1) == 0;
  }

  public boolean isVolatile() {
     return ((flags >>> StorageFlags.VOLATILE_BIT) & 1) != 0;
  }

  public boolean isRemote() {
     return ((flags >>> StorageFlags.REMOTE_BIT) & 1) != 0;
  }

  public boolean isDriveBased() {
     return ((flags >>> StorageFlags.DRIVE_BASED_BIT) & 1) != 0;
  }

  public int getOrderValue() {
     return ORDERED_MAP.get(this);
  }
  
  public String getAbbreviation() {
     return ABBREV_MAP.get(this);
  }
  
  public static List<StorageType> asList() {
    return Arrays.asList(VALUES);
  }

  public static List<StorageType> getMovableTypes() {
    return getNonTransientTypes();
  }

  public static List<StorageType> getTypesSupportingQuota() {
    return getNonTransientTypes();
  }

  public static StorageType parseStorageType(int i) {
    return VALUES[i];
  }

  public static StorageType parseStorageType(String s) {
    StorageType type = STRINGS_MAP.get(StringUtils.toUpperCase(s));
    if (type == null)
       throw new IllegalArgumentException("StorageType has no enum with name " + s);

    return type;
  }

  public static StorageType[] valuesOrdered() {
    return ORDERED_VALUES;
  }

  public static StorageType[] valuesCustomOrdered() {
     return CUSTOM_ORDER;
   }

  private static List<StorageType> getNonTransientTypes() {
    List<StorageType> nonTransientTypes = new ArrayList<>();
    for (StorageType t : VALUES) {
      if ( ((t.flags >>> StorageFlags.TRANSIENT_BIT) & 1) == 0 ) {
        nonTransientTypes.add(t);
      }
    }
    return nonTransientTypes;
  }
}
