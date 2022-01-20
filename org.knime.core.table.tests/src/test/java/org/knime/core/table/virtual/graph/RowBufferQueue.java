package org.knime.core.table.virtual.graph;

import java.util.ArrayDeque;
import java.util.Objects;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class RowBufferQueue<R> {

    private final ArrayDeque<R> items;

    /** Number of elements in the queue */
    private int count;

    /** Main lock guarding all accesses */
    private final ReentrantLock lock;

    /** Condition for waiting take()s */
    private final Condition notEmpty;

    private long pendingIndex;

    /**
     * Creates an {@code RowQueue} with the given initial capacity.
     *
     * @param capacity the capacity of this queue
     */
    public RowBufferQueue(final int capacity) {
        items = new ArrayDeque<>(capacity);
        lock = new ReentrantLock();
        notEmpty = lock.newCondition();
    }

    public RowBufferQueue() {
        this(64);
    }

    /**
     * Retrieves and removes the row buffer at the head of this queue, waiting if
     * necessary until one becomes available.
     * <p>
     * The returned buffer will be used to store data for the row with the specified
     * {@code rowIndex}. If there is only one buffer currently in the queue, wait until
     * {@link #pendingIndex pendingIndex}{@code ==rowIndex}. This is done to avoid
     * filling all available row buffers and then not being able to accept the next row
     * needed to make progress.
     *
     * @return the head of this queue
     * @throws InterruptedException if interrupted while waiting
     */
    public R take(final long rowIndex) throws InterruptedException {
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            while (true) {
                if (count > 1 || (count == 1 && rowIndex == pendingIndex)) {
                    --count;
                    return items.remove();
                }
                notEmpty.await();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Inserts the specified element at the tail of this queue.
     */
    public void put(R rowBuffer) {
        Objects.requireNonNull(rowBuffer);
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            items.add(rowBuffer);
            ++count;
            notEmpty.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Sets the index of the next row we are waiting for to carry on delivering
     * sorted rows to consumer. This is used to avoid filling all available row buffers
     * and then not being able to accept this row.
     */
    public void setPendingIndex(final long pendingIndex)  {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            this.pendingIndex = pendingIndex;
            notEmpty.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the number of elements in this queue.
     *
     * @return the number of elements in this queue
     */
    public int size() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            return count;
        } finally {
            lock.unlock();
        }
    }
}
