/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any questions please contact the copyright holder:
 * website: www.knime.com
 * email: contact@knime.com
 * ---------------------------------------------------------------------
 *
 * History
 *   Created on Jul 27, 2021 by Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
package org.knime.core.table.cursor;

import java.io.IOException;

import org.knime.core.table.access.BufferedAccesses;
import org.knime.core.table.access.BufferedAccesses.BufferedAccessRow;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.DelegatingReadAccesses;
import org.knime.core.table.virtual.DelegatingReadAccesses.DelegatingReadAccessRow;

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