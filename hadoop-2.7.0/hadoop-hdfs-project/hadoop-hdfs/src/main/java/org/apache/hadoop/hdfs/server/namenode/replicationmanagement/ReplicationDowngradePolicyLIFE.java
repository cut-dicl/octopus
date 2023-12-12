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
 * This class includes the LIFE policy (from PACMan publication) to select a file
 * for downgrade.
 * 
 * 
 * @author elena.kakoulli
 */
public class ReplicationDowngradePolicyLIFE extends
		ReplicationDowngradePolicyThreshold {
	
	private long windowBasedAging;
	private SortedWeightedTree<WeightedLFRUNode> oldListLFU;		  // list with files older than the window sorted based on frequency
	private SortedWeightedTree<WeightedFileSizeNode> newListSize; // list with newer files than the window sorted based on file size
	private SortedWeightedTree<WeightedLFRUNode> newListLRU;		  // list with newer files than the window sorted based on file access time

	@Override
	protected void initialize(Configuration conf, FSDirectory dir, DatanodeStatistics stats) {
		super.initialize(conf, dir, stats);	

		windowBasedAging = 60l * 60 * 1000 * conf.getInt(
				DFSConfigKeys.DFS_REPLICATION_DOWNGRADE_POLICY_WINDOW_BASED_AGING_KEY,
				DFSConfigKeys.DFS_REPLICATION_DOWNGRADE_POLICY_WINDOW_BASED_AGING_DEFAULT);

		this.oldListLFU = new SortedWeightedTree<WeightedLFRUNode>(new WeightedLFRUNode.LFUComparator());
		this.newListSize = new SortedWeightedTree<WeightedFileSizeNode>();
		this.newListLRU = new SortedWeightedTree<WeightedLFRUNode>(new WeightedLFRUNode.LRUComparator());

      long now = System.currentTimeMillis();
      Iterator<INodeWithAdditionalFields> iter = dir.getINodeMap().getMapIterator();
      while (iter.hasNext()) {
         INodeWithAdditionalFields n = iter.next();
         if (n.isFile()) {
            INodeFile file = n.asFile();
            if ((now - file.getAccessTime()) > windowBasedAging) {
               oldListLFU.addNode(new WeightedLFRUNode(file));
            } else {
               newListSize.addNode(new WeightedFileSizeNode(file));
               newListLRU.addNode(new WeightedLFRUNode(file));
            }
         }
      }
	}

	@Override
	protected INodeFile selectFileToDowngrade(StorageType type) {
      // traverse the newListLRU in ascending order and move
      // any files older than the window into the oldListLFU.
      // These files must be removed from newListSize as well.
      Iterator<WeightedNode> nodes = newListLRU.ascIter();
      while (nodes.hasNext()) {
         long now = System.currentTimeMillis();
         WeightedNode node = nodes.next();
         if ((now - node.getWeight()) > windowBasedAging) {
            oldListLFU.addNode(node);
            nodes.remove();
            newListSize.deleteNode(node.getFile());
         } else {
            // All files in newListLRU from now on are newer than the window
            break;
         }
      }
		
      // If oldListLFU is not empty, return LFU.
      // Otherwise, return the largest file from newListSize.
      INodeFile downgradeFile = null;
      Iterator<INodeFile> oldfiles = oldListLFU.ascFileIter();
      while (oldfiles.hasNext()) {
         downgradeFile = oldfiles.next();
         if (ReplicationVector.GetReplication(type, downgradeFile.getReplicationVector()) > 0
               && !downgradeFile.isUnderConstruction()) {
            return downgradeFile;
         }
      }
		
      Iterator<INodeFile> newfiles = newListSize.descFileIter();
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
            newListLRU.addNode(delNode);
            newListSize.addNode(new WeightedFileSizeNode(file));
         } else if (newListLRU.existWeightedNode(file.getId())) {
            // Update the file in new LRU list. No need to update Size list.
            newListLRU.updateNode(file);
         } else {
            // On file addition: the file is added in the two new lists
            newListSize.addNode(new WeightedFileSizeNode(file));
            newListLRU.addNode(new WeightedLFRUNode(file));
         }
      }
	}

	@Override
	protected void onFileDelete(INodeFile file) {
      if (file != null) {
         if (oldListLFU.existWeightedNode(file.getId())) {
            oldListLFU.deleteNode(file);
         } else if (newListLRU.existWeightedNode(file.getId())) {
            newListSize.deleteNode(file);
            newListLRU.deleteNode(file);
         }
      }
	}

}
