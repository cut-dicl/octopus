package org.apache.hadoop.hdfs.server.datanode.fsdataset.tier;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetFactory;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.memory.FsDatasetMemFactory;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A factory for creating {@link FsDatasetTier} objects.
 */

public class FsDatasetTierFactory extends FsDatasetSpi.Factory<FsDatasetTier> {

   static private EnumMap<StorageType, Class<? extends FsDatasetSpi.Factory<?>>> TierMapFactory = new EnumMap<StorageType, Class<? extends FsDatasetSpi.Factory<?>>>(
         StorageType.class);

   static {
      // Initialize the default factories per storage type
      TierMapFactory.put(StorageType.DISK, FsDatasetFactory.class);
      TierMapFactory.put(StorageType.SSD, FsDatasetFactory.class);
      TierMapFactory.put(StorageType.ARCHIVE, FsDatasetFactory.class);
      TierMapFactory.put(StorageType.RAM_DISK, null);
      TierMapFactory.put(StorageType.MEMORY, FsDatasetMemFactory.class);
      TierMapFactory.put(StorageType.DISK_H, FsDatasetFactory.class);
      TierMapFactory.put(StorageType.SSD_H, FsDatasetFactory.class);
      TierMapFactory.put(StorageType.REMOTE, null);
   }

   /** Create a new object of FsDatasetTier */
   @Override
   public FsDatasetTier newInstance(DataNode datanode, DataStorage storage,
         Configuration conf) throws IOException {

      FsDatasetTier fsDSTier = new FsDatasetTier();

      // parse configuration file to identify storage types
      Collection<String> rawLocations = conf
            .getTrimmedStringCollection(DFS_DATANODE_DATA_DIR_KEY);

      // Organize locations per storage type
      EnumMap<StorageType, List<String>> typeToLocation = new EnumMap<StorageType, List<String>>(
            StorageType.class);

      for (String locationString : rawLocations) {
         StorageLocation location = StorageLocation.parse(locationString);

         List<String> locs = typeToLocation.get(location.getStorageType());
         if (locs == null) {
            locs = new ArrayList<String>(rawLocations.size());
            typeToLocation.put(location.getStorageType(), locs);
         }

         locs.add(locationString);
      }

      if (typeToLocation.containsKey(StorageType.RAM_DISK)) {
         // RAM_DISK requires the default backing storage
         List<String> defaultLocs = typeToLocation.get(StorageType.DEFAULT);
         if (defaultLocs == null || StorageType.DEFAULT.isTransient()
               || !StorageType.DEFAULT.isDriveBased())
            throw new IOException("RAM_DISK requires a storage of type "
                  + StorageType.DEFAULT
                  + " that is drive-based but not transient");

         defaultLocs.addAll(typeToLocation.get(StorageType.RAM_DISK));
         typeToLocation.remove(StorageType.RAM_DISK);
      }

      // Create one dataset per storage type
      for (Entry<StorageType, List<String>> entry : typeToLocation.entrySet()) {

         StorageType type = entry.getKey();
         List<String> listLocs = entry.getValue();
         Configuration newConf = new Configuration(conf);

         newConf.setStrings(DFS_DATANODE_DATA_DIR_KEY,
               listLocs.toArray(new String[listLocs.size()]));

         // Create the dataset factory and instance
         final Class<?> clazz = newConf.getClass(
               DFSConfigKeys.DFS_DATANODE_FSDATASET_FACTORY_KEY + "."
                     + type.toString(), TierMapFactory.get(type),
               FsDatasetSpi.Factory.class);

         @SuppressWarnings("unchecked")
         FsDatasetSpi.Factory<? extends FsVolumeSpi> newFactoryInstance = (FsDatasetSpi.Factory<? extends FsVolumeSpi>) ReflectionUtils
               .newInstance(clazz, newConf);

         FsDatasetSpi<? extends FsVolumeSpi> fsDsInstance = newFactoryInstance
               .newInstance(datanode, storage, newConf);

         fsDSTier.addTier(type, fsDsInstance);
      }

      return fsDSTier;
   }

}
