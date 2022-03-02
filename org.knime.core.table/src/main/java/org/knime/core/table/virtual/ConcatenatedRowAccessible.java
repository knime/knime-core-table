/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   Apr 3, 2021 (marcel): created
 */
package org.knime.core.table.virtual;

import static java.util.stream.Collectors.toList;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.knime.core.table.access.DelegatingReadAccesses;
import org.knime.core.table.access.DelegatingReadAccesses.DelegatingReadAccessRow;
import org.knime.core.table.cursor.LookaheadCursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.row.Selection;
import org.knime.core.table.row.Selection.ColumnSelection;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.spec.ConcatenateTransformSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the operation specified by {@link ConcatenateTransformSpec}.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
final class ConcatenatedRowAccessible implements LookaheadRowAccessible {

    private final List<LookaheadRowAccessible> m_delegates;

    private final ColumnarSchema m_schema;

    ConcatenatedRowAccessible(final List<RowAccessible> tablesToConcatenate) {
        m_delegates = tablesToConcatenate.stream()//
            .map(RowAccessibles::toLookahead)//
            .collect(toList());
        m_schema = ColumnarSchemas.concatenate(tablesToConcatenate.stream()//
            .map(RowAccessible::getSchema)//
            .collect(toList()));
    }

    /**
     * @return the concatenated row accessibles
     */
    public List<? extends RowAccessible> getConcatenatedRowAccessibles() {//NOSONAR
        return m_delegates;
    }

    @Override
    public ColumnarSchema getSchema() {
        return m_schema;
    }

    @Override
    public LookaheadCursor<ReadAccessRow> createCursor() {
        return new ConcatenatedRowCursor(m_delegates, getSchema());
    }

    @Override
    public LookaheadCursor<ReadAccessRow> createCursor(final Selection selection) {
        var cursor = new ConcatenatedRowCursor(m_delegates, getSchema(), selection.columns());
        return selection.rows().allSelected() ? cursor : new SlicedCursor(cursor, selection.rows());
    }

    @Override
    public void close() throws IOException {
        for (LookaheadRowAccessible delegate : m_delegates) {
            delegate.close();
        }
    }

    // TODO: the implementation of the cursor here is conceptually pretty similar to the multi-chunk cursor in
    // knime-core-columnar. Consolidate some of the "multiple-underlying-partitions" handling logic?
    private static final class ConcatenatedRowCursor implements LookaheadCursor<ReadAccessRow> {

        private static final Logger LOGGER = LoggerFactory.getLogger(ConcatenatedRowCursor.class);

        private final Iterator<LookaheadRowAccessible> m_delegateTables;

        private final DelegatingReadAccessRow m_access;

        private final Selection m_selection;

        private LookaheadCursor<ReadAccessRow> m_currentDelegateCursor;

        public ConcatenatedRowCursor(final List<LookaheadRowAccessible> inputs, final ColumnarSchema schema, final ColumnSelection columnSelection) {
            m_delegateTables = inputs.iterator();
            m_access = DelegatingReadAccesses.createDelegatingReadAccessRow(schema, columnSelection);
            m_selection = Selection.all().retainColumns(columnSelection);
            m_currentDelegateCursor = findNextNonEmptyCursor();
        }

        public ConcatenatedRowCursor(final List<LookaheadRowAccessible> inputs, final ColumnarSchema schema) {
            this(inputs, schema, Selection.all().columns());
        }

        private LookaheadCursor<ReadAccessRow> findNextNonEmptyCursor() {
            while (m_delegateTables.hasNext()) {
                @SuppressWarnings("resource")
                var cursor = m_delegateTables.next().createCursor(m_selection);
                if (cursor.canForward()) {
                    m_access.setDelegateAccess(cursor.access());
                    return cursor;
                } else {
                    closeWithDebug(cursor);
                }
            }
            return null;
        }

        private static void closeWithDebug(final Closeable closeable) {
            try {
                closeable.close();
            } catch (IOException e) {
                LOGGER.debug("Failed to close delegate cursor.", e);
            }
        }

        @Override
        public ReadAccessRow access() {
            return m_access;
        }

        @Override
        public boolean forward() {
            if (canForward()) {
                return m_currentDelegateCursor.forward();
            } else {
                return false;
            }
        }

        @Override
        public boolean canForward() {
            if (m_currentDelegateCursor == null) {
                return false;
            } else if (!m_currentDelegateCursor.canForward()) {
                closeWithDebug(m_currentDelegateCursor);
                m_currentDelegateCursor = findNextNonEmptyCursor();
                return canForward();
            } else {
                return true;
            }
        }

        @Override
        public void close() throws IOException {
            if (m_currentDelegateCursor != null) {
                m_currentDelegateCursor.close();
            }
        }
    }

}
