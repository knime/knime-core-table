package org.knime.core.table.virtual.graph;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingDeque;

public class RowOrderList<R> {
    // TODO: Recycle OrderEntry objects

    static class OrderEntry<R> {
        long firstRowIndex;

        long lastRowIndex;

        final List<R> buffers = new ArrayList<>();

        // constructor for a buffered output row
        OrderEntry(final long index, final R buffer) {
            this.firstRowIndex = index;
            this.lastRowIndex = index;
            this.buffers.add(buffer);
        }

        // constructor for a skipped output row
        OrderEntry(final long index) {
            this.firstRowIndex = index;
            this.lastRowIndex = index;
        }
    }

    private final BlockingDeque<R> outputQueue;

    private final ArrayList<OrderEntry<R>> entries = new ArrayList<>();
            // TODO: Would it be more efficient to replace this with a (growable) RingBuffer? (But start by profiling...)

    private final RowBufferQueue<R> rowBufferQueue;

    private long pendingIndex;

    RowOrderList(final BlockingDeque<R> outputQueue, final RowBufferQueue<R> rowBufferQueue, final long pendingIndex) {
        this.outputQueue = outputQueue;
        this.rowBufferQueue = rowBufferQueue;
        this.pendingIndex = pendingIndex;
        rowBufferQueue.setPendingIndex(pendingIndex);
    }

    void accept(final long rowIndex, final R row) throws InterruptedException {

        if (tryOutput(rowIndex, row))
            return;

        // find the entry before which this index will be inserted
        // by comparing with firstRowIndex
        int i = findInsertionIndex(rowIndex);

        // can rowIndex be prepended to the OrderEntry at index i?
        if (i < entries.size()) {
            final OrderEntry<R> entry = entries.get(i);
            if (entry.firstRowIndex == rowIndex + 1) {
                entry.firstRowIndex = rowIndex;
                entry.buffers.add(0, row);
                tryMerge(i - 1);
                return;
            }
        }

        // can rowIndex be appended to the OrderEntry at index i-1?
        if (i > 0) {
            final OrderEntry<R> entry = entries.get(i - 1);
            if (entry.lastRowIndex == rowIndex - 1) {
                entry.lastRowIndex = rowIndex;
                entry.buffers.add(row);
                tryMerge(i);
                return;
            }
        }

        // insert a new entry at i
        entries.add(i, new OrderEntry(rowIndex, row));
    }

    void skip(final long rowIndex) throws InterruptedException {

        if (tryOutput(rowIndex))
            return;

        // find the entry before which this index will be inserted
        // by comparing with firstRowIndex
        int i = findInsertionIndex(rowIndex);

        // can rowIndex be prepended to the OrderEntry at index i?
        if (i < entries.size()) {
            final OrderEntry<R> entry = entries.get(i);
            if (entry.firstRowIndex == rowIndex + 1) {
                entry.firstRowIndex = rowIndex;
                tryMerge(i - 1);
                return;
            }
        }

        // can rowIndex be appended to the OrderEntry at index i-1?
        if (i > 0) {
            final OrderEntry<R> entry = entries.get(i - 1);
            if (entry.lastRowIndex == rowIndex - 1) {
                entry.lastRowIndex = rowIndex;
                tryMerge(i);
                return;
            }
        }

        // insert a new entry at i
        entries.add(i, new OrderEntry<>(rowIndex));
    }

    // try to merge entries at i and i + 1
    private void tryMerge(int i) {
        if (i >= 0 && i + 1 < entries.size()) {
            final OrderEntry<R> e1 = entries.get(i);
            final OrderEntry<R> e2 = entries.get(i + 1);
            if (e1.lastRowIndex + 1 == e2.firstRowIndex) {
                e1.lastRowIndex = e2.lastRowIndex;
                e1.buffers.addAll(e2.buffers);
                entries.remove(i + 1);
            }
        }
    }

    //   TODO: replace by binary search
    private int findInsertionIndex(final long rowIndex) {
        for (int i = 0; i < entries.size(); i++) {
            if (rowIndex < entries.get(i).firstRowIndex) {
                return i;
            }
        }
        return entries.size();
    }

    private boolean tryOutput(final long rowIndex) throws InterruptedException {
        return tryOutput(rowIndex, null);
    }

    private boolean tryOutput(final long rowIndex, final R row) throws InterruptedException {
        if (rowIndex != pendingIndex) {
            return false;
        }

        if (row != null) {
            outputQueue.put(row);
        } // else skip rowIndex

        ++pendingIndex;

        if (!entries.isEmpty()) {
            if (entries.get(0).firstRowIndex == pendingIndex) {
                final OrderEntry<R> entry = entries.remove(0);
                for (R buffer : entry.buffers) {
                    outputQueue.put(buffer);
                }
                pendingIndex = entry.lastRowIndex + 1;
            }
        }

        rowBufferQueue.setPendingIndex(pendingIndex);

        return true;
    }
}
