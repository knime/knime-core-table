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

class DefaultSelection implements Selection {

    private final ColumnSelection m_columns;

    private final RowRangeSelection m_rows;

    static final Selection ALL = new DefaultSelection(new DefaultColumnSelection(), new DefaultRowRangeSelection());

    public DefaultSelection(final ColumnSelection columns, final RowRangeSelection rows) {
        this.m_columns = columns;
        this.m_rows = rows;
    }

    @Override
    public ColumnSelection columns() {
        return m_columns;
    }

    @Override
    public RowRangeSelection rows() {
        return m_rows;
    }

    @Override
    public Selection retainColumns(final int... columns) {
        return new DefaultSelection(columns().retain(columns), m_rows);
    }

    @Override
    public Selection retainColumns(final ColumnSelection selection) {
        return new DefaultSelection(m_columns.retain(selection), m_rows);
    }

    @Override
    public Selection retainRows(final long from, final long to) {
        return new DefaultSelection(m_columns, m_rows.retain(from, to));
    }

    @Override
    public Selection retainRows(final RowRangeSelection selection) {
        return new DefaultSelection(m_columns, m_rows.retain(selection));
    }

    @Override
    public Selection retain(final Selection selection) {
        return new DefaultSelection(m_columns.retain(selection.columns()), m_rows.retain(selection.rows()));
    }

    @Override
    public int hashCode() {
        return m_columns.hashCode() * 31 + m_rows.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Selection)) {
            return false;
        }
        Selection that = (Selection)obj;
        return m_columns.equals(that.columns()) && m_rows.equals(that.rows());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DefaultSelection{");
        sb.append(m_columns);
        sb.append(", ").append(m_rows);
        sb.append('}');
        return sb.toString();
    }
}
