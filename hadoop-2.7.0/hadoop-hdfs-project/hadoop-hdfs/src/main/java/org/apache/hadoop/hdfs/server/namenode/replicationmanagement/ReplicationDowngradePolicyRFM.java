package org.apache.hadoop.hdfs.server.namenode.replicationmanagement;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ReplicationVector;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStatistics;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;

/**
 * A downgrade policy based on RFM. See {@link RFMCollection} for details.
 * 
 * @author herodotos.herodotou
 */
public class ReplicationDowngradePolicyRFM extends
      ReplicationDowngradePolicyThreshold {

   protected RFMCollection rfm;
   private boolean rfmOwner;

   @Override
   protected void initialize(Configuration conf, FSDirectory dir,
         DatanodeStatistics stats) {
      super.initialize(conf, dir, stats);

      rfm = RFMCollection.getSharedRFM();
      rfmOwner = (RFMCollection.getSharedCount() == 1);
      if (rfmOwner) {
         ArrayList<INodeFile> fileList = ReplicationManagementUtils
               .collectSortedFiles(dir);
         for (INodeFile file : fileList)
            rfm.addFile(file);
      }
   }

   @Override
   protected INodeFile selectFileToDowngrade(StorageType type) {
      Iterator<INodeFile> files = rfm.ascFileIter();
      while (files.hasNext()) {
         INodeFile file = files.next();
         if (ReplicationVector
               .GetReplication(type, file.getReplicationVector()) > 0
               && !file.isUnderConstruction()) {
            return file;
         }
      }

      return null;
   }

   @Override
   protected void onFileInfoUpdate(INodeFile file) {
      if (rfmOwner && file != null) {
         if (rfm.containsFile(file))
            rfm.accessFile(file);
         else
            rfm.addFile(file);
      }
   }

   @Override
   protected void onFileDelete(INodeFile file) {
      if (rfmOwner && file != null)
         rfm.removeFile(file);
   }
}
