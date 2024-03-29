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
 *   Created on May 21, 2021 by dietzc
 */
package org.knime.core.table.virtual;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.knime.core.table.cursor.Cursors;
import org.knime.core.table.cursor.LookaheadCursor;
import org.knime.core.table.row.LookaheadRowAccessible;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.row.Selection;
import org.knime.core.table.row.Selection.RowRangeSelection;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.spec.MapTransformSpec.MapperFactory;
import org.knime.core.table.virtual.spec.RowFilterTransformSpec.RowFilterFactory;

/**
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public final class RowAccessibles {

    private RowAccessibles() {
    }

    public static RowAccessible append(final RowAccessible in, final RowAccessible... other) {
        final ArrayList<RowAccessible> list = new ArrayList<>();
        if (in instanceof AppendedRowAccessible) {
            list.addAll(((AppendedRowAccessible)in).getAppendedTables());
        } else {
            list.add(in);
        }
        list.addAll(Arrays.asList(other));
        return new AppendedRowAccessible(list);
    }

    public static RowAccessible appendMissing(final RowAccessible in, final ColumnarSchema appened) {
        return new AppendedMissingRowAccessible(in, appened);
    }

    public static RowAccessible concatenate(final RowAccessible in, final RowAccessible... other) {
        final ArrayList<RowAccessible> list = new ArrayList<>();
        if (in instanceof ConcatenatedRowAccessible) {
            list.addAll(((ConcatenatedRowAccessible)in).getConcatenatedRowAccessibles());
        } else {
            list.add(in);
        }
        list.addAll(Arrays.asList(other));
        return new ConcatenatedRowAccessible(list);
    }

    // TODO rename to selectColumns()
    public static RowAccessible filter(final RowAccessible in, final int[] selection) {
        return new ColumnFilteredRowAccessible(in, selection);
    }

    public static RowAccessible slice(final RowAccessible in, final RowRangeSelection selection) {
        return new SlicedRowAccessible(in, selection.fromIndex(), selection.toIndex());
    }

    public static RowAccessible map(final RowAccessible in, final int[] columnIndices, final MapperFactory mapperFactory) {
        return new MappedRowAccessible(in, columnIndices, mapperFactory);
    }

    public static RowAccessible filterRows(final RowAccessible in, final int[] columnIndices, final RowFilterFactory filterFactory)
    {
        return new FilteredRowAccessible(in, columnIndices, filterFactory);
    }

    public static LookaheadRowAccessible toLookahead(final RowAccessible rowAccessible) {
        if (rowAccessible instanceof LookaheadRowAccessible) {
            return (LookaheadRowAccessible)rowAccessible;
        } else {
            return new AdapterLookaheadRowAccessible(rowAccessible);
        }
    }

    private static final class AdapterLookaheadRowAccessible implements LookaheadRowAccessible {

        private final RowAccessible m_delegate;

        AdapterLookaheadRowAccessible(final RowAccessible delegate) {
            m_delegate = delegate;
        }

        @Override
        public ColumnarSchema getSchema() {
            return m_delegate.getSchema();
        }

        @Override
        public long size() {
            return m_delegate.size();
        }

        @Override
        public void close() throws IOException {
            m_delegate.close();
        }

        @SuppressWarnings("resource")
        @Override
        public LookaheadCursor<ReadAccessRow> createCursor() {
            return Cursors.toLookahead(m_delegate.getSchema(), m_delegate.createCursor());
        }

        @SuppressWarnings("resource")
        @Override
        public LookaheadCursor<ReadAccessRow> createCursor(final Selection selection) {
            return Cursors.toLookahead(m_delegate.getSchema(), m_delegate.createCursor(selection), selection.columns());
        }
    }
}
