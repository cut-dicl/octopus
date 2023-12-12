package org.apache.hadoop.hdfs.server.datanode.fsdataset.memory;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;


/**
 * A factory for creating {@link FsDatasetMem} objects.
 */
public class FsDatasetMemFactory extends FsDatasetSpi.Factory<FsDatasetMem> {

	 @Override
	  public FsDatasetMem newInstance(DataNode datanode,
	      DataStorage storage, Configuration conf) throws IOException {
	    return new FsDatasetMem(datanode, storage, conf);
	  }
}
