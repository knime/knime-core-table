package org.knime.core.table.virtual;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.knime.core.table.access.MissingAccesses;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.cursor.Cursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.DelegatingReadAccesses.DelegatingReadAccess;
import org.knime.core.table.virtual.spec.AppendTransformSpec;

/**
 * Implementation of the operation specified by {@link AppendTransformSpec}.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
class AppendedRowAccessible implements RowAccessible {

    private final List<RowAccessible> m_delegateTables;

    private final ColumnarSchema m_schema;

    private final int[] m_tableOffsets;

    public AppendedRowAccessible(final List<RowAccessible> tablesToAppend) {
        this(tablesToAppend,
            ColumnarSchemas.append(tablesToAppend.stream().map((i) -> i.getSchema()).collect(Collectors.toList())));
    }

    AppendedRowAccessible(final List<RowAccessible> tablesToAppend, final ColumnarSchema schema) {
        m_delegateTables = new ArrayList<>(tablesToAppend);
        m_schema = schema;
        m_tableOffsets = new int[tablesToAppend.size()];
        int currentOffset = 0;
        for (int i = 0; i < tablesToAppend.size(); i++) {
            m_tableOffsets[i] = currentOffset;
            @SuppressWarnings("resource") // Tables will be closed when this instance is being closed.
            final RowAccessible table = tablesToAppend.get(i);
            currentOffset += table.getSchema().numColumns();
        }
    }

    /**
     * @return all appended tables
     */
    List<? extends RowAccessible> getAppendedTables() {
        return m_delegateTables;
    }

    @Override
    public ColumnarSchema getSchema() {
        return m_schema;
    }

    @SuppressWarnings("resource") // Delegate cursors will be closed upon closing of the returned cursor.
    @Override
    public Cursor<ReadAccessRow> createCursor() {
        @SuppressWarnings("unchecked")
        final Cursor<ReadAccessRow>[] delegateCursors = new Cursor[m_delegateTables.size()];
        for (int i = 0; i < delegateCursors.length; i++) {
            delegateCursors[i] = m_delegateTables.get(i).createCursor();
        }
        return new AppendedCursor(delegateCursors, m_tableOffsets, m_schema);
    }

    @Override
    public void close() throws IOException {
        for (final RowAccessible table : m_delegateTables) {
            table.close();
        }
    }

    private static final class AppendedCursor implements Cursor<ReadAccessRow> {

        private final Cursor<ReadAccessRow>[] m_delegateCursors;

        private final AppendedReadAccessRow m_access;

        public AppendedCursor(final Cursor<ReadAccessRow>[] delegateCursors, final int[] tableOffsets,
            final ColumnarSchema schema) {
            m_delegateCursors = delegateCursors;
            final ReadAccessRow[] delegateAccesses = new ReadAccessRow[m_delegateCursors.length];
            for (int i = 0; i < delegateAccesses.length; i++) {
                delegateAccesses[i] = m_delegateCursors[i].access();
            }
            m_access = new AppendedReadAccessRow(delegateAccesses, tableOffsets, schema);
        }

        @Override
        public ReadAccessRow access() {
            return m_access;
        }

        @Override
        public boolean forward() {
            boolean anyCursorForwarded = false;
            for (int i = 0; i < m_delegateCursors.length; i++) {
                final boolean cursorForwarded = m_delegateCursors[i].forward();
                if (!cursorForwarded) {
                    m_access.setColumnAccessesOfTableToMissing(i);
                }
                anyCursorForwarded |= cursorForwarded;
            }
            return anyCursorForwarded;
        }

        @Override
        public void close() throws IOException {
            for (final Cursor<ReadAccessRow> cursor : m_delegateCursors) {
                cursor.close();
            }
        }

        private static final class AppendedReadAccessRow implements ReadAccessRow {

            private final DelegatingReadAccess<?>[] m_columnAccesses;

            private final int[] m_tableOffsets;

            private final ColumnarSchema m_schema;

            public AppendedReadAccessRow(final ReadAccessRow[] delegateAccesses, final int[] tableOffsets,
                final ColumnarSchema schema) {
                m_tableOffsets = tableOffsets;
                m_schema = schema;
                m_columnAccesses = createColumnAccesses(delegateAccesses, tableOffsets, schema);
            }

            private static DelegatingReadAccess<?>[] createColumnAccesses(final ReadAccessRow[] delegateRowAccesses,
                final int[] tableOffsets, final ColumnarSchema schema) {
                final DelegatingReadAccess<?>[] columnAccesses = new DelegatingReadAccess[schema.numColumns()];
                int delegateTableIndex = 0;
                int delegateColumnIndex = 0;
                for (int i = 0; i < columnAccesses.length; i++) {
                    while (delegateTableIndex < tableOffsets.length && tableOffsets[delegateTableIndex] <= i) {
                        delegateTableIndex++;
                    }
                    delegateTableIndex--;
                    delegateColumnIndex = i - tableOffsets[delegateTableIndex];
                    final ReadAccess delegateColumnAccess =
                        delegateRowAccesses[delegateTableIndex].getAccess(delegateColumnIndex);
                    @SuppressWarnings("unchecked") // Type safety is ensured by data-spec matching at runtime.
                    final DelegatingReadAccess<ReadAccess> delegatingColumnAccess =
                        (DelegatingReadAccess<ReadAccess>)DelegatingReadAccesses
                            .createDelegatingAccess(schema.getSpec(i));
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
                    @SuppressWarnings("unchecked") // Type safety is ensured by data-spec matching at runtime.
                    final DelegatingReadAccess<ReadAccess> delegatingAccess =
                        (DelegatingReadAccess<ReadAccess>)m_columnAccesses[i];
                    delegatingAccess.setDelegateAccess(missingAccess);
                }
            }

            @Override
            public int size() {
                return m_columnAccesses.length;
            }

            @Override
            public <A extends ReadAccess> A getAccess(final int index) {
                @SuppressWarnings("unchecked")
                final A casted = (A)m_columnAccesses[index];
                return casted;
            }
        }
    }
}
