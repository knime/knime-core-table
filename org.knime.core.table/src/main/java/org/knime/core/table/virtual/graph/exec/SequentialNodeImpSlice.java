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
package org.knime.core.table.virtual.graph.exec;

import java.io.IOException;

import org.knime.core.table.access.ReadAccess;

class SequentialNodeImpSlice implements SequentialNodeImp {
    private final SequentialNodeImp predecessor;

    /**
     * Index of the first row (inclusive) to include in the slice.
     * Row indices are wrt to the rows provided by the predecessor.
     */
    private final long m_from;

    /**
     * Index of the last row (exclusive) to include in the slice.
     * Row indices are wrt to the rows provided by the predecessor.
     */
    private final long m_to;

    /**
     * Index of the row that will be provided after the next call to
     * to {@code forward()}.
     * <p>
     * For example, if the slice starts at {@code m_from=0}, then before the
     * first {@code forward()} it would be 0. Then {@code forward()} provides
     * values for row 0, and increments to {@code m_nextRowIndex=1}. Row indices
     * are wrt to the rows provided by the predecessor.
     */
    private long m_nextRowIndex;

    SequentialNodeImpSlice(final SequentialNodeImp predecessor, final long from, final long to) {
        this.predecessor = predecessor;
        m_from = from;
        m_to = to;
        m_nextRowIndex = 0;
    }

    @Override
    public ReadAccess getOutput(final int i) {
        // SLICE doesn't have inputs or outputs
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isValid() {
        // SLICE doesn't have inputs or outputs
        throw new UnsupportedOperationException();
    }

    @Override
    public void create() {
        predecessor.create();
    }

    @Override
    public boolean forward() {
        forwardToStart();
        if (m_nextRowIndex < m_to) {
            m_nextRowIndex++;
            return predecessor.forward();
        } else {
            return false;
        }
    }

    @Override
    public boolean canForward() {
        forwardToStart();
        if (m_nextRowIndex < m_to) {
            return predecessor.canForward();
        } else {
            return false;
        }
    }

    private void forwardToStart() {
        while (m_nextRowIndex < m_from) {
            predecessor.forward();
            m_nextRowIndex++;
        }
    }

    @Override
    public void close() throws IOException {
        predecessor.close();
    }
}
