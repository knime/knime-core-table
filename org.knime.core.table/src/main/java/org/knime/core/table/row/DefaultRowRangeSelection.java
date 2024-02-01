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
package org.knime.core.table.row;

import org.knime.core.table.row.Selection.RowRangeSelection;

class DefaultRowRangeSelection implements RowRangeSelection {

    private final long m_from;

    private final long m_to;

    static final RowRangeSelection ALL = new DefaultRowRangeSelection();

    public DefaultRowRangeSelection() {
        this(-1, -1);
    }

    public DefaultRowRangeSelection(final long from, final long to) {
        this.m_from = from;
        this.m_to = Math.max(from, to); // if (to < from), the row range is empty
    }

    @Override
    public boolean allSelected() {
        return m_from < 0;
    }

    @Override
    public boolean allSelected(final long from, final long to) {
        return allSelected() || (from >= m_from && to <= m_to);
    }

    @Override
    public long fromIndex() {
        return m_from;
    }

    @Override
    public long toIndex() {
        return m_to;
    }

    @Override
    public RowRangeSelection retain(final long from, final long to) {
        if (allSelected()) {
            return new DefaultRowRangeSelection(from, to);
        } else if (from < 0) {
            return this;
        } else {
            return new DefaultRowRangeSelection(//
                    Math.min(this.m_to, this.m_from + from),//
                    Math.min(this.m_to, this.m_from + to));
        }
    }

    @Override
    public int hashCode() {
        return Long.hashCode(m_from) * 31 + Long.hashCode(m_to);
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof RowRangeSelection)) { // NOSONAR
            return false;
        }
        final RowRangeSelection that = (RowRangeSelection)obj;
        if (allSelected() != that.allSelected()) {
            return false;
        }
        if (allSelected() && that.allSelected()) {
            return true;
        }
        return m_from == that.fromIndex() && m_to == that.toIndex();
    }

    @Override
    public String toString() {
        if (allSelected()) {
            return "select all rows";
        } else {
            final StringBuilder sb = new StringBuilder("select rows");
            sb.append(" from=").append(m_from);
            sb.append(" to=").append(m_to);
            return sb.toString();
        }
    }
}
