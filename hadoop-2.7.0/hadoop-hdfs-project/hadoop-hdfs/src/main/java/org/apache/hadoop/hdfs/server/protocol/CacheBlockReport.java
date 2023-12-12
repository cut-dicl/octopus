package org.apache.hadoop.hdfs.server.protocol;

import java.util.ArrayList;
import java.util.List;

/**
 * Cache block report for a Datanode storage
 */
public class CacheBlockReport {

    private final DatanodeStorage storage;
    private List<Long> blockIds;

    public CacheBlockReport(DatanodeStorage storage, List<Long> blockIds) {
        this.storage = storage;
        this.blockIds = new ArrayList<Long>(blockIds);
    }

    public DatanodeStorage getStorage() {
        return storage;
    }

    public List<Long> getBlockIds() {
        return blockIds;
    }
}
