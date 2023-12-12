package org.apache.hadoop.hdfs.server.namenode.replicationmanagement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStatistics;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * This interface is used for file replication downgrade policies.
 * 
 * @author elena.kakoulli
 */

@InterfaceAudience.Private
public abstract class ReplicationDowngradePolicy {
    static final Log LOG = LogFactory.getLog(ReplicationDowngradePolicy.class);

    /**
     * Used to setup a ReplicationDowngradePolicy object.
     * 
     * @param conf
     *            the configuration object
     * @param dir
     *            FSDirectory from FSNamesystem
     * @param stats
     *            datanodes statistics
     */
    abstract protected void initialize(Configuration conf, FSDirectory dir, DatanodeStatistics stats);

    /**
     * Used to trigger the downgrade process.
     * 
     * @param type
     *            the storage type
     * @param pendingSize
     *            total size of pending file replications 
     * @return true/false
     * 
     */
    abstract protected boolean triggerDowngrade(StorageType type, long pendingSize);

    /**
     * Used to trigger to continue the downgrade process.
     * 
     * @param type
     *            the storage type
     * @param pendingSize
     *            total size of pending file replications 
     * @return true/false
     * 
     */
    abstract protected boolean continueDowngrade(StorageType type, long pendingSize);
    
    /**
     * Used to select a file to downgrade.
     * 
     * @param type
     *            the storage type
     */
    abstract protected INodeFile selectFileToDowngrade(StorageType type);

    /**
     * Used to downgrade a file.
     * 
     * @param file
     *            the file to downgrade
     * @param type
     *            the current storage type 
     * @return the new RV
     */

    abstract protected long downgradeFile(INodeFile file, StorageType type);

    /**
     * Used to update.
     * 
     * @param file
     *            the INode file
     */
    abstract protected void onFileInfoUpdate(INodeFile file);
    
    /**
     * Used to delete.
     * 
     * @param file
     *            the INode file
     */
    abstract protected void onFileDelete(INodeFile file);
    
    /**
     * Get an instance of the configured Replication Downgrade Policy based on the
     * the configuration property
     * {@link DFSConfigKeys#DFS_REPLICATION_DOWNGRADE_POLICY_CLASSNAME_KEY}.
     * 
     * @param conf
     *            the configuration object
     * @param dir
     *            FSDirectory from FSNamesystem
     * @param Stats
     *            datanodes statistics
     * @return an instance of ReplicationDowngradePolicy
     */
    public static ReplicationDowngradePolicy getInstance(Configuration conf, FSDirectory dir, DatanodeStatistics stats) {
        final Class<? extends ReplicationDowngradePolicy> policyClass = conf.getClass(
                DFSConfigKeys.DFS_REPLICATION_DOWNGRADE_POLICY_CLASSNAME_KEY,
                DFSConfigKeys.DFS_REPLICATION_DOWNGRADE_POLICY_CLASSNAME_DEFAULT, ReplicationDowngradePolicy.class);
        final ReplicationDowngradePolicy policy = ReflectionUtils.newInstance(policyClass, conf);
        policy.initialize(conf, dir, stats);
        return policy;
    }
}
