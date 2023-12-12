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
 * This interface is used for file replication upgrade policies.
 * 
 * @author elena.kakoulli
 */

@InterfaceAudience.Private
public abstract class ReplicationUpgradePolicy {
    static final Log LOG = LogFactory.getLog(ReplicationUpgradePolicy.class);

    /**
     * Used to setup a ReplicationUpgradePolicy object.
     * 
     * @param conf
     *            the configuration object
     * @param dir
     *            FSDirectory from FSNamesystem
     * @param Stats
     *            datanodes statistics
     */
    abstract protected void initialize(Configuration conf, FSDirectory dir, DatanodeStatistics stats);

    /**
     * Used to trigger the upgrade process.
     * 
     * @param type
     *            the storage type
     * @return true/false
     * 
     */
    abstract protected boolean triggerUpgrade(StorageType type, INodeFile file);

    /**
     * Used to trigger to continue the upgrade process.
     * 
     * @param type
     *            the storage type
     * @return true/false
     * 
     */
    abstract protected boolean continueUpgrade(StorageType type);

    /**
     * Used to select a file to upgrade.
     * 
     * @param file
     *            the specified file
     * @param type
     *            the storage type
     * @return the INode file
     */
    abstract protected INodeFile selectFileToUpgrade(INodeFile file, StorageType type);

    /**
     * Used to upgrade a file.
     * 
     * @param file
     *            the INode file
     * @param type
     *            the storage type           
     * @return the replication vector
     */
    abstract protected long upgradeFile(INodeFile file, StorageType type);

    /**
     * Used to update LRU and increase the number of accesses of a file or delete it.
     * 
     * @param file
     *            the INodeFile which has an access
     * 
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
     * Get an instance of the configured File Replication Policy based on the
     * the configuration property
     * {@link DFSConfigKeys#DFS_REPLICATION_UPGRADE_POLICY_CLASSNAME_KEY}.
     * 
     * @param conf
     *            the configuration object
     * @param dir
     *            FSDirectory from FSNamesystem
     * @param Stats
     *            datanodes statistics
     * @return an instance of ReplicationUpgradePolicy
     */
    public static ReplicationUpgradePolicy getInstance(Configuration conf, FSDirectory dir, DatanodeStatistics stats) {
        final Class<? extends ReplicationUpgradePolicy> policyClass = conf.getClass(
                DFSConfigKeys.DFS_REPLICATION_UPGRADE_POLICY_CLASSNAME_KEY,
                DFSConfigKeys.DFS_REPLICATION_UPGRADE_POLICY_CLASSNAME_DEFAULT, ReplicationUpgradePolicy.class);
        final ReplicationUpgradePolicy policy = ReflectionUtils.newInstance(policyClass, conf);
        policy.initialize(conf, dir, stats);
        return policy;
    }
}
