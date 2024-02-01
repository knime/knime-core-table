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
 */
package org.knime.core.table.virtual;

import java.io.IOException;

import org.knime.core.table.cursor.LookaheadCursor;
import org.knime.core.table.row.LookaheadRowAccessible;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.row.Selection;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.spec.SliceTransformSpec;

/**
 * Implementation of the operation specified by {@link SliceTransformSpec}.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */

/*
 * TODO in case of batch read stores we can be much smarter about "slicing" data, as we have random access on rows.
 */
final class SlicedRowAccessible implements LookaheadRowAccessible {

    private final LookaheadRowAccessible m_delegateTable;

    private final long m_from;

    private final long m_to;

    private final long m_size;

    public SlicedRowAccessible(final RowAccessible tableToSlice, final long from, final long to) {
        m_delegateTable = RowAccessibles.toLookahead(tableToSlice);
        m_from = from;
        m_to = to;

        final long s = m_delegateTable.size();
        m_size = s < 0 ? s : Math.max(0, Math.min(s, to) - from);
    }

    @Override
    public ColumnarSchema getSchema() {
        return m_delegateTable.getSchema();
    }

    @Override
    public LookaheadCursor<ReadAccessRow> createCursor() {
        return m_delegateTable.createCursor(Selection.all().retainRows(m_from, m_to));
    }

    @Override
    public LookaheadCursor<ReadAccessRow> createCursor(final Selection selection) {
        return m_delegateTable.createCursor(Selection.all().retainRows(m_from, m_to).retain(selection));
    }

    @Override
    public long size() {
        return m_size;
    }

    @Override
    public void close() throws IOException {
        m_delegateTable.close();
    }
}
