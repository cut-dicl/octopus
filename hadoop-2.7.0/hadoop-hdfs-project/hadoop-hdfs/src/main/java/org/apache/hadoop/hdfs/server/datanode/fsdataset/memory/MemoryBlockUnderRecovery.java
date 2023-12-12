package org.apache.hadoop.hdfs.server.datanode.fsdataset.memory;

import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;

public class MemoryBlockUnderRecovery extends MemoryBlock {

	private MemoryBlock original; 	// the original replica that needs to be recovered
	private long recoveryId; 		// recovery id; it is also the generation stamp
									// that the replica will be bumped to after
									// recovery

	public MemoryBlockUnderRecovery(MemoryBlock replica, long recoveryId) {
		super(replica);
		if (replica.getState() != ReplicaState.FINALIZED 
		      && replica.getState() != ReplicaState.RBW
				&& replica.getState() != ReplicaState.RWR) {
			throw new IllegalArgumentException("Cannot recover replica: " + replica);
		}
		
		this.original = replica;
		this.recoveryId = recoveryId;
	}

	/**
	 * Copy constructor.
	 * 
	 * @param from
	 *            where to copy from
	 */
	public MemoryBlockUnderRecovery(MemoryBlockUnderRecovery from) {
		super(from);
		this.original = from.getOriginalReplica();
		this.recoveryId = from.getRecoveryID();
	}

	/**
	 * Get the recovery id
	 * 
	 * @return the generation stamp that the replica will be bumped to
	 */
	public long getRecoveryID() {
		return recoveryId;
	}

	/**
	 * Set the recovery id
	 * 
	 * @param recoveryId
	 *            the new recoveryId
	 */
	public void setRecoveryID(long recoveryId) {
		if (recoveryId > this.recoveryId) {
			this.recoveryId = recoveryId;
		} else {
         throw new IllegalArgumentException("The new recovery id: " + recoveryId
               + " must be greater than the current one: " + this.recoveryId);
		}
	}

	/**
	 * Get the original replica that's under recovery
	 * 
	 * @return the original replica under recovery
	 */
	public MemoryBlock getOriginalReplica() {
		return original;
	}

	@Override // Replica
	public ReplicaState getState() {
		return ReplicaState.RUR;
	}

	@Override
	public long getVisibleLength() {
		return original.getVisibleLength();
	}

	@Override
	public long getBytesOnDisk() {
		return original.getBytesOnDisk();
	}

	@Override // org.apache.hadoop.hdfs.protocol.Block
	public void setBlockId(long blockId) {
		super.setBlockId(blockId);
		original.setBlockId(blockId);
	}

	@Override // org.apache.hadoop.hdfs.protocol.Block
	public void setGenerationStamp(long gs) {
		super.setGenerationStamp(gs);
		original.setGenerationStamp(gs);
	}

	@Override // org.apache.hadoop.hdfs.protocol.Block
	public void setNumBytes(long numBytes) {
		super.setNumBytes(numBytes);
		original.setNumBytes(numBytes);
	}

	@Override // Object
	public boolean equals(Object o) {
		return super.equals(o);
	}

	@Override // Object
	public int hashCode() {
		return super.hashCode();
	}

	@Override
	public String toString() {
		return super.toString() + "\n  recoveryId=" + recoveryId + "\n  original=" + original;
	}

	public ReplicaRecoveryInfo createInfo() {
      return new ReplicaRecoveryInfo(original.getBlockId(),
            original.getBytesOnDisk(), original.getGenerationStamp(),
            original.getState());
	}
}
