package org.knime.core.table.virtual;

import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.knime.core.table.access.DelegatingReadAccesses;
import org.knime.core.table.access.DelegatingReadAccesses.DelegatingReadAccess;
import org.knime.core.table.access.MissingAccesses;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.cursor.Cursor;
import org.knime.core.table.cursor.LookaheadCursor;
import org.knime.core.table.row.LookaheadRowAccessible;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.row.Selection;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.spec.AppendTransformSpec;

/**
 * Implementation of the operation specified by {@link AppendTransformSpec}.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
final class AppendedRowAccessible implements LookaheadRowAccessible {

    private final List<LookaheadRowAccessible> m_delegateTables;

    private final ColumnarSchema m_schema;

    private final int[] m_tableOffsets;

    private final long m_size;

    public AppendedRowAccessible(final List<RowAccessible> tablesToAppend) {
        this(tablesToAppend, ColumnarSchemas.append(tablesToAppend.stream()//
            .map(RowAccessible::getSchema)//
            .collect(toList())));
    }

    AppendedRowAccessible(final List<RowAccessible> tablesToAppend, final ColumnarSchema schema) {
        m_delegateTables = tablesToAppend.stream()//
            .map(RowAccessibles::toLookahead)//
            .collect(toList());//
        m_schema = schema;
        m_tableOffsets = new int[tablesToAppend.size()];
        int currentOffset = 0;//NOSONAR
        long size = 0;
        for (int i = 0; i < tablesToAppend.size(); i++) {//NOSONAR
            m_tableOffsets[i] = currentOffset;
            @SuppressWarnings("resource") // Tables will be closed when this instance is being closed.
            final RowAccessible table = tablesToAppend.get(i);
            currentOffset += table.getSchema().numColumns();

            // If any source table doesn't know its size, size of the appended table is also
            // unknown. Otherwise, the size of the appended table is the size of the largest
            // input table.
            if ( table.size() >= 0 && size >= 0 ) {
                size = Math.max(table.size(), size);
            } else {
                size = -1;
            }
        }
        m_size = size;
    }

    /**
     * @return all appended tables
     */
    List<? extends RowAccessible> getAppendedTables() {//NOSONAR
        return m_delegateTables;
    }

    @Override
    public ColumnarSchema getSchema() {
        return m_schema;
    }

    @Override
    public LookaheadCursor<ReadAccessRow> createCursor() {
        return new AppendedCursor();
    }

    @Override
    public LookaheadCursor<ReadAccessRow> createCursor(final Selection selection) {
        final int numTables = m_delegateTables.size();
        final Selection[] delegateSelections = new Selection[numTables];
        if (selection.columns().allSelected()) {
            Arrays.fill(delegateSelections, selection);
        } else {
            for (int t = 0; t < numTables; t++) {
                final int fromIndex = m_tableOffsets[t];
                final int toIndex = t + 1 < numTables ? m_tableOffsets[t + 1] : m_schema.numColumns();
                final int[] selected = selection.columns().getSelected(fromIndex, toIndex);
                final int[] delegateCols = new int[selected.length];
                Arrays.setAll(delegateCols, i -> selected[i] - fromIndex);
                delegateSelections[t] = Selection.all().retainRows(selection.rows()).retainColumns(delegateCols);
            }
        }
        return new AppendedCursor(delegateSelections);
    }

    @Override
    public long size() {
        return m_size;
    }

    @Override
    public void close() throws IOException {
        for (final RowAccessible table : m_delegateTables) {
            table.close();
        }
    }

    private final class AppendedCursor implements LookaheadCursor<ReadAccessRow> {

        private final LookaheadCursor<ReadAccessRow>[] m_delegateCursors;

        private final AppendedReadAccessRow m_access;

        @SuppressWarnings("unchecked")
        AppendedCursor() {
            m_delegateCursors = m_delegateTables.stream()//
                .map(LookaheadRowAccessible::createCursor)//
                .toArray(LookaheadCursor[]::new);
            final ReadAccessRow[] delegateAccesses = Stream.of(m_delegateCursors)//
                .map(Cursor::access)//
                .toArray(ReadAccessRow[]::new);
            m_access = new AppendedReadAccessRow(delegateAccesses);
        }

        AppendedCursor(final Selection[] delegateSelections) {
            final int numTables = m_delegateTables.size();
            m_delegateCursors = new LookaheadCursor[numTables];
            Arrays.setAll(m_delegateCursors, i -> m_delegateTables.get(i).createCursor(delegateSelections[i]));
            final ReadAccessRow[] delegateAccesses = new ReadAccessRow[numTables];
            Arrays.setAll(delegateAccesses, i -> m_delegateCursors[i].access());
            m_access = new AppendedReadAccessRow(delegateAccesses);
        }

        @Override
        public ReadAccessRow access() {
            return m_access;
        }

        @Override
        public boolean forward() {
            var anyCursorForwarded = false;
            for (int i = 0; i < m_delegateCursors.length; i++) {//NOSONAR
                final boolean cursorForwarded = m_delegateCursors[i].forward();
                if (!cursorForwarded) {
                    m_access.setColumnAccessesOfTableToMissing(i);
                }
                anyCursorForwarded |= cursorForwarded;
            }
            return anyCursorForwarded;
        }

        @Override
        public boolean canForward() {
            for (var cursor : m_delegateCursors) {
                if (cursor.canForward()) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public void close() throws IOException {
            for (final Cursor<ReadAccessRow> cursor : m_delegateCursors) {
                cursor.close();
            }
        }

        private final class AppendedReadAccessRow implements ReadAccessRow {

            private final DelegatingReadAccess[] m_columnAccesses;

            public AppendedReadAccessRow(final ReadAccessRow[] delegateAccesses) {
                m_columnAccesses = createColumnAccesses(delegateAccesses);
            }

            private DelegatingReadAccess[] createColumnAccesses(final ReadAccessRow[] delegateRowAccesses) {
                final var columnAccesses = new DelegatingReadAccess[m_schema.numColumns()];
                int delegateTableIndex = 0;//NOSONAR
                int delegateColumnIndex = 0;//NOSONAR
                for (int i = 0; i < columnAccesses.length; i++) {//NOSONAR
                    while (delegateTableIndex < m_tableOffsets.length && m_tableOffsets[delegateTableIndex] <= i) {
                        delegateTableIndex++;
                    }
                    delegateTableIndex--;
                    delegateColumnIndex = i - m_tableOffsets[delegateTableIndex];
                    final ReadAccess delegateColumnAccess =
                        delegateRowAccesses[delegateTableIndex].getAccess(delegateColumnIndex);
                    final var delegatingColumnAccess =
                        DelegatingReadAccesses.createDelegatingAccess(m_schema.getSpec(i));
                    delegatingColumnAccess.setDelegateAccess(delegateColumnAccess);
                    columnAccesses[i] = delegatingColumnAccess;
                }
                return columnAccesses;
            }

            private void setColumnAccessesOfTableToMissing(final int tableIndex) {
                final int fromInclusive = m_tableOffsets[tableIndex];
                final int toExclusive = tableIndex + 1 < m_tableOffsets.length //
                    ? m_tableOffsets[tableIndex + 1] //
                    : m_schema.numColumns();
                for (int i = fromInclusive; i < toExclusive; i++) {
                    final ReadAccess missingAccess = MissingAccesses.getMissingAccess(m_schema.getSpec(i));
                    m_columnAccesses[i].setDelegateAccess(missingAccess);
                }
            }

            @Override
            public int size() {
                return m_columnAccesses.length;
            }

            @Override
            public <A extends ReadAccess> A getAccess(final int index) {
                @SuppressWarnings("unchecked")
                final A casted = (A)m_columnAccesses[index]; // NOSONAR
                return casted;
            }
        }
    }
}
