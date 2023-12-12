package org.apache.hadoop.hdfs.server.datanode.fsdataset.memory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Represents a block of memory (essentially a byte array) implemented as a
 * queue of ByteBuffer objects. It is implemented in a way that supports the
 * single-writer/multiple-readers access model.
 * 
 * The basic unit of data is a ByteBuffer. Hence, the data can only be appended
 * on ByteBuffer at a time.
 */
public class ByteBufferQueue {

	// Data members
	private ConcurrentLinkedQueue<ByteBuffer> buffers;
	private long size; // sum of bytes of ByteBuffers
	private long capacity; // sum of capacities of ByteBuffers
	private MemoryStats memoryStats;

	public ByteBufferQueue(MemoryStats memoryStats) {
		this.buffers = new ConcurrentLinkedQueue<ByteBuffer>();
		this.size = 0L;
		this.capacity = 0L;
		this.memoryStats = memoryStats;
	}

	/**
	 * Append a ByteBuffer in the queue. This method also flips the ByteBuffer
	 * for reading
	 * 
	 * @param bb
	 */
	public void appendByteByffer(ByteBuffer bb) {
		synchronized (buffers) {
			bb.flip();
			buffers.offer(bb);
			size += bb.limit();
			capacity += bb.capacity();

			// refresh the memoryStats
			memoryStats.increment(bb.limit(), bb.capacity());
		}
	}

	/**
	 * @return a read-only iterator for iterating the queue
	 */
	public Iterator<ByteBuffer> iterator() {
		synchronized (buffers) {
			return Collections.unmodifiableCollection(buffers).iterator();
		}
	}

	/**
	 * @return size of data in bytes
	 */
	public long size() {
		synchronized (buffers) {
			return size;
		}
	}

	/**
	 * @return capacity of queue in bytes
	 */
	public long capacity() {
		synchronized (buffers) {
			return capacity;
		}
	}

	/**
	 * Rewind the ByteBufferQueue to the specified position. All data after the
	 * position are gone. The last ByteBuffer with the position is returned.
	 * Note that the last ByteBuffer is no longer in the queue and needs to be
	 * re-appended.
	 * 
	 * Throws IndexOutOfBoundsException if position is negative or greater than
	 * size.
	 * 
	 * @param position
	 *            the requested position
	 * @param pool
	 *            the pool for releasing deleted byte buffers
	 * @return The byte buffer ready to be written at the requested position.
	 *         The buffer may be anything between empty and full. Returns null
	 *         if the queue is empty.
	 */
	public ByteBuffer rewind(long position, ByteBufferPool pool) {

		synchronized (buffers) {

			if (position < 0 || position > size)
				throw new IndexOutOfBoundsException();

			long oldSize = size;
			long oldCapacity = capacity;
			ByteBuffer lastBB = null;
			Iterator<ByteBuffer> iter = buffers.iterator();
			long cummSize = 0;

			// Find the ByteBuffer with the requested position
			while (iter.hasNext() && cummSize <= position) {
				lastBB = iter.next();
				cummSize += lastBB.limit();
			}

			if (lastBB == null)
				return null; // The queue is empty

			// Prepare the indexes of the last ByteBuffer
			int newPos = lastBB.limit() - (int) (cummSize - position);
			lastBB.limit(lastBB.capacity());
			lastBB.position(newPos);
			iter.remove();
			capacity -= lastBB.capacity();

			// Remove the remaining ByteBuffers from the queue
			while (iter.hasNext()) {
				ByteBuffer bb = iter.next();
				pool.releaseByteBuffer(bb);
				iter.remove();
				capacity -= bb.capacity();

			}

			size = position - newPos;

			// refresh the memoryStats
			memoryStats.decrement(oldSize - size, oldCapacity - capacity);

			return lastBB;
		}
	}

	/**
	 * Delete all data and return the ByteBuffers back to the pool
	 * 
	 * @param pool
	 */
	public void delete(ByteBufferPool pool) {

		synchronized (buffers) {
			// Release all ByteBuffers back to the pool
			Iterator<ByteBuffer> iter = buffers.iterator();
			while (iter.hasNext()) {
				ByteBuffer byteBuffer = iter.next();
            memoryStats.decrement(byteBuffer.limit(), byteBuffer.capacity());

            pool.releaseByteBuffer(byteBuffer);
				iter.remove();
			}

			capacity = 0;
			size = 0;
		}
	}

	/**
	 * @return A new deep copy of this ByteBufferQueue
	 */
	public ByteBufferQueue copy() {

		ByteBufferQueue newBBQ = new ByteBufferQueue(memoryStats);

		synchronized (buffers) {
			Iterator<ByteBuffer> iter = buffers.iterator();

			while (iter.hasNext()) {
				ByteBuffer oldBB = iter.next();
				byte[] oldArray = oldBB.array();
				byte[] newArray = Arrays.copyOf(oldArray, oldArray.length);
				ByteBuffer newBB = ByteBuffer.wrap(newArray);
				newBB.position(oldBB.limit());
				newBBQ.appendByteByffer(newBB);
			}
		}

		return newBBQ;
	}
	
}
