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
 *   Created on Jul 27, 2021 by Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
package org.knime.core.table.cursor;

import java.io.IOException;

import org.knime.core.table.access.BufferedAccesses;
import org.knime.core.table.access.BufferedAccesses.BufferedAccessRow;
import org.knime.core.table.access.DelegatingReadAccesses;
import org.knime.core.table.access.DelegatingReadAccesses.DelegatingReadAccessRow;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.Selection.ColumnSelection;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * {@link LookaheadCursor} based on a non-lookahead {@link Cursor} that reads one row ahead in order to implement
 * {@link LookaheadCursor#canForward()}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class BufferingLookaheadCursor implements LookaheadCursor<ReadAccessRow> {

    private final BufferedAccessRow[] m_buffers = new BufferedAccessRow[2];

    private final DelegatingReadAccessRow m_delegator;

    private final Cursor<ReadAccessRow> m_cursor;

    private boolean m_nextRowBuffered = false;

    private boolean m_canForward = true;

    private int m_bufferIdx = 0;

    BufferingLookaheadCursor(final ColumnarSchema schema, final Cursor<ReadAccessRow> cursor) {
        m_cursor = cursor;
        m_delegator = DelegatingReadAccesses.createDelegatingReadAccessRow(schema);
        m_buffers[0] = BufferedAccesses.createBufferedAccessRow(schema);
        m_buffers[1] = BufferedAccesses.createBufferedAccessRow(schema);
    }

    BufferingLookaheadCursor(final ColumnarSchema schema, final Cursor<ReadAccessRow> cursor,
        final ColumnSelection columnSelection) {
        m_cursor = cursor;
        m_delegator = DelegatingReadAccesses.createDelegatingReadAccessRow(schema, columnSelection);
        m_buffers[0] = BufferedAccesses.createBufferedAccessRow(schema, columnSelection);
        m_buffers[1] = BufferedAccesses.createBufferedAccessRow(schema, columnSelection);
    }

    @Override
    public ReadAccessRow access() {
        return m_delegator;
    }

    @Override
    public boolean forward() {
        if (canForward()) {
            switchBuffers();
            return true;
        } else {
            return false;
        }
    }

    private void switchBuffers() {
        m_bufferIdx = getOtherBufferIdx();
        m_delegator.setDelegateAccess(getBufferInUse());
        m_nextRowBuffered = false;
    }

    @Override
    public void close() throws IOException {
        // TODO does it make sense to add a clear on BufferedAccess?
        m_cursor.close();
    }

    private void bufferNextRow() {
        if (!m_nextRowBuffered) {
            m_canForward = m_cursor.forward();
            if (m_canForward) {
                // Note: all cells of a BufferedAccessRow need to be populated when advancing to a new row,
                //       which we do here, so no need to call setMissing() on all cells first.
                getFreeBuffer().setFrom(m_cursor.access());
            }
            m_nextRowBuffered = true;
        }
    }

    private BufferedAccessRow getFreeBuffer() {
        return m_buffers[getOtherBufferIdx()];
    }

    private BufferedAccessRow getBufferInUse() {
        return m_buffers[m_bufferIdx];
    }

    private int getOtherBufferIdx() {
        return (m_bufferIdx + 1) % 2;
    }

    @Override
    public boolean canForward() {
        bufferNextRow();
        return m_canForward;
    }

}