package org.knime.core.table.virtual.graph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class ConcurrentCursors
{
	public static void main( String[] args )
	{
		final List< Row > rows = generateData( 10, 0.5, 10, 1000 );
		for ( Row row : rows )
			System.out.println( row );

		Collections.shuffle( rows, new Random( 2l ) );
		OrderList orderList = new OrderList();
		for ( Row row : rows )
		{
//			System.out.println();
//			System.out.println();
//			System.out.println( "row = " + row );
//			System.out.println();
			if ( row.skipped )
				orderList.skip( row.rowIndex );
			else
				orderList.accept( row.rowIndex, row );
//			System.out.println( "orderList = \n" + orderList );
		}
	}

	static List< Row > generateData(final int nRows, final double skipProbablility, final int minProcessingTime, final int maxProcessingTime)
	{
		final List< Row > rows = new ArrayList<>();
		final Random random = new Random( 1l );

		for ( int i = 0; i < nRows; i++ )
		{
			final boolean skipped = random.nextDouble() < skipProbablility;
			final int processingTime = minProcessingTime + ( int ) ( ( maxProcessingTime - minProcessingTime ) * random.nextDouble() );
			rows.add( new Row( i, skipped, processingTime ) );
		}

		return rows;
	}

	static class Row
	{
		private final int rowIndex;
		private final boolean skipped;
		private final int processingTime;

		public Row( final int rowIndex, final boolean skipped, final int processingTime )
		{
			this.rowIndex = rowIndex;
			this.skipped = skipped;
			this.processingTime = processingTime;
		}

		@Override
		public String toString()
		{
			return "Row{" +
					rowIndex +
					(skipped ? " (skipped)" : "")+
					", t=" + processingTime +
					'}';
		}
	}

	static class OutputQueue
	{
		final List< Row > rows = new ArrayList<>();

		void accept( final Row row )
		{
			rows.add( row );
			System.out.println( "outputting " + row );
		}
	}

	static class OrderEntry
	{
		int firstRowIndex;
		int lastRowIndex;
		final List<Row> buffers = new ArrayList<>();

		// constructor for a buffered output row
		OrderEntry( int index, Row buffer )
		{
			this.firstRowIndex = index;
			this.lastRowIndex = index;
			this.buffers.add( buffer );
		}

		// constructor for a skipped output row
		OrderEntry( int index )
		{
			this.firstRowIndex = index;
			this.lastRowIndex = index;
		}

		@Override
		public String toString()
		{
			return "OrderEntry{" +
					"firstRowIndex=" + firstRowIndex +
					", lastRowIndex=" + lastRowIndex +
					", buffers=" + buffers +
					'}';
		}
	}

	static class OrderList
	{
		private final OutputQueue outputQueue = new OutputQueue();

		private int pendingIndex = 0;

		private final ArrayList<OrderEntry> entries = new ArrayList<>();
		// TODO: Would it be more efficient to replace this with a (growable) RingBuffer?
		// 		(But start by profiling...)

		void accept(final int rowIndex, final Row row) {

			if (tryOutput(rowIndex, row))
				return;

			// find the entry before which this index will be inserted
			// by comparing with firstRowIndex
			int i = findInsertionIndex(rowIndex);

			// can rowIndex be prepended to the OrderEntry at index i?
			if (i < entries.size()) {
				final OrderEntry entry = entries.get(i);
				if (entry.firstRowIndex == rowIndex + 1) {
					entry.firstRowIndex = rowIndex;
					entry.buffers.add(0, row);
					tryMerge(i - 1);
					return;
				}
			}

			// can rowIndex be appended to the OrderEntry at index i-1?
			if (i > 0) {
				final OrderEntry entry = entries.get(i - 1);
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

		void skip(int rowIndex) {

			if (tryOutput(rowIndex))
				return;

			// find the entry before which this index will be inserted
			// by comparing with firstRowIndex
			int i = findInsertionIndex(rowIndex);

			// can rowIndex be prepended to the OrderEntry at index i?
			if (i < entries.size()) {
				final OrderEntry entry = entries.get(i);
				if (entry.firstRowIndex == rowIndex + 1) {
					entry.firstRowIndex = rowIndex;
					tryMerge(i - 1);
					return;
				}
			}

			// can rowIndex be appended to the OrderEntry at index i-1?
			if (i > 0) {
				final OrderEntry entry = entries.get(i - 1);
				if (entry.lastRowIndex == rowIndex - 1) {
					entry.lastRowIndex = rowIndex;
					tryMerge(i);
					return;
				}
			}

			// insert a new entry at i
			entries.add(i, new OrderEntry(rowIndex));
		}

		// try to merge entries at i and i + 1
		private void tryMerge(int i) {
			if (i >= 0 && i + 1 < entries.size()) {
				final OrderEntry e1 = entries.get(i);
				final OrderEntry e2 = entries.get(i + 1);
				if (e1.lastRowIndex + 1 == e2.firstRowIndex) {
					e1.lastRowIndex = e2.lastRowIndex;
					e1.buffers.addAll(e2.buffers);
					entries.remove(i + 1);
				}
			}
		}

		//   TODO: replace by binary search
		private int findInsertionIndex(int rowIndex) {
			for (int i = 0; i < entries.size(); i++) {
				if (rowIndex < entries.get(i).firstRowIndex) {
					return i;
				}
			}
			return entries.size();
		}

		private boolean tryOutput(final int rowIndex) {
			return tryOutput(rowIndex, null);
		}

		private boolean tryOutput(final int rowIndex, final Row row) {
			if (rowIndex != pendingIndex) {
				return false;
			}

			if (row != null) {
				outputQueue.accept(row);
			} // else skip rowIndex

			++pendingIndex;

			if (!entries.isEmpty()) {
				if (entries.get(0).firstRowIndex == pendingIndex) {
					final OrderEntry entry = entries.remove(0);
					entry.buffers.forEach(outputQueue::accept);
					pendingIndex = entry.lastRowIndex + 1;
				}
			}

			// TODO: set new pendingIndex to RowQueue

			return true;
		}

		@Override
		public String toString() {
			final StringBuffer sb = new StringBuffer();
			for (OrderEntry entry : entries) {
				sb.append("  " + entry + "\n");
			}
			return sb.toString();
		}
	}

}
