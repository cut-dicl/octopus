package org.apache.hadoop.hdfs.server.namenode.replicationmanagement;

import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ReplicationVector;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStatistics;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeWithAdditionalFields;

/**
 * This class includes the LFU-F (from PACMan publication) to select a file
 * for downgrade.
 * 
 * 
 * @author elena.kakoulli
 */
public class ReplicationDowngradePolicyLFU_F extends
		ReplicationDowngradePolicyThreshold {

	private long windowBasedAging;
	protected SortedWeightedTree<WeightedLFRUNode> oldListLFU;	// list with files older than the window sorted based on frequency
	protected SortedWeightedTree<WeightedLFRUNode> newListLFU;	// list with newer files than the window sorted based on access count
	protected SortedWeightedTree<WeightedLFRUNode> newListLRU;	// list with newer files than the window sorted based on file access time

	@Override
	protected void initialize(Configuration conf, FSDirectory dir, DatanodeStatistics stats) {
		super.initialize(conf, dir, stats);	

		windowBasedAging = 60l * 60 * 1000 * conf.getInt(
				DFSConfigKeys.DFS_REPLICATION_DOWNGRADE_POLICY_WINDOW_BASED_AGING_KEY,
				DFSConfigKeys.DFS_REPLICATION_DOWNGRADE_POLICY_WINDOW_BASED_AGING_DEFAULT);

		this.oldListLFU = new SortedWeightedTree<WeightedLFRUNode>(new WeightedLFRUNode.LFUComparator());
		this.newListLFU = new SortedWeightedTree<WeightedLFRUNode>(new WeightedLFRUNode.LFUComparator());
		this.newListLRU = new SortedWeightedTree<WeightedLFRUNode>(new WeightedLFRUNode.LRUComparator());

      long now = System.currentTimeMillis();
      Iterator<INodeWithAdditionalFields> iter = dir.getINodeMap().getMapIterator();
      while (iter.hasNext()) {
         INodeWithAdditionalFields n = iter.next();
         if (n.isFile()) {
            WeightedLFRUNode node = new WeightedLFRUNode(n.asFile());
            if ((now - node.getLastAccessTime()) > windowBasedAging) {
               oldListLFU.addNode(node);
            } else {
               newListLFU.addNode(node);
               newListLRU.addNode(node);
            }
         }
      }
	}

	@Override
	protected INodeFile selectFileToDowngrade(StorageType type) {
      // traverse the newListLRU in ascending order and move
      // any files older than the window into the oldListLFU.
      // These files must be removed from newListLFU as well.
      Iterator<WeightedNode> nodes = newListLRU.ascIter();
      while (nodes.hasNext()) {
         long now = System.currentTimeMillis();
         WeightedNode node = nodes.next();
         if ((now - node.getWeight()) > windowBasedAging) {
            oldListLFU.addNode(node);
            newListLFU.deleteNode(node.getFile());
            nodes.remove();
         } else {
            // All files in newListLRU from now on are newer than the window
            break;
         }
      }

      // If oldListLFU is not empty, return LFU.
      // Otherwise, return the least frequent file from newListLFU.
      INodeFile downgradeFile = null;
      Iterator<INodeFile> oldfiles = oldListLFU.ascFileIter();
      while (oldfiles.hasNext()) {
         downgradeFile = oldfiles.next();
         if (ReplicationVector.GetReplication(type, downgradeFile.getReplicationVector()) > 0
               && !downgradeFile.isUnderConstruction()) {
            return downgradeFile;
         }
      }

      Iterator<INodeFile> newfiles = newListLFU.ascFileIter();
      while (newfiles.hasNext()) {
         downgradeFile = newfiles.next();
         if (ReplicationVector.GetReplication(type, downgradeFile.getReplicationVector()) > 0
               && !downgradeFile.isUnderConstruction()) {
            return downgradeFile;
         }
      }

      return null;
	}

	@Override
	protected void onFileInfoUpdate(INodeFile file) {
      if (file != null) {
         // On file access: if the file is in oldListLFU,
         // it is removed from there and added in the two new lists.
         if (oldListLFU.existWeightedNode(file.getId())) {
            WeightedNode delNode = oldListLFU.deleteNode(file);
            delNode.updateWeight();
            newListLFU.addNode(delNode);
            newListLRU.addNode(delNode);
         } else if (newListLRU.existWeightedNode(file.getId())) {
            // Update the file in both new lists by removing and adding it back
            WeightedNode node = newListLRU.deleteNode(file);
            newListLFU.deleteNode(file);
            node.updateWeight();
            newListLFU.addNode(node);
            newListLRU.addNode(node);
         } else {
            // On file addition: the file is added in the two new lists
            WeightedLFRUNode node = new WeightedLFRUNode(file);
            newListLFU.addNode(node);
            newListLRU.addNode(node);
         }
      }
	}

	@Override
	protected void onFileDelete(INodeFile file) {
      if (file != null) {
         if (oldListLFU.existWeightedNode(file.getId())) {
            oldListLFU.deleteNode(file);
         } else if (newListLRU.existWeightedNode(file.getId())) {
            newListLFU.deleteNode(file);
            newListLRU.deleteNode(file);
         }
      }
	}

}
