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

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.IntStream;

import org.knime.core.table.row.Selection.ColumnSelection;

class DefaultColumnSelection implements ColumnSelection {

    private final int[] m_cols;

    static final ColumnSelection ALL = new DefaultColumnSelection();

    public DefaultColumnSelection(final int... cols) {
        if (cols == null || cols.length == 0) {
            this.m_cols = null;
        } else {
            final int[] cols2 = cols.clone();
            Arrays.sort(cols2);
            this.m_cols = removeDuplicates(cols2);
        }
    }

    private DefaultColumnSelection(final int[] cols, final boolean ignore) {
        this.m_cols = cols;
    }

    @Override
    public boolean allSelected() {
        return m_cols == null;
    }

    @Override
    public boolean allSelected(final int fromIndex, final int toIndex) {
        if (fromIndex > toIndex) {
            throw new IllegalArgumentException("fromIndex (==" + fromIndex + ") > toIndex (==" + toIndex + ")");
        }
        if (fromIndex < 0) {
            throw new IllegalArgumentException("fromIndex (==" + fromIndex + ") < 0");
        }
        if (allSelected() || fromIndex == toIndex) {
            return true;
        } else {
            // For all columns in the given range to be selected:
            // - fromIndex should be in the selection (at some index i)
            // - toIndex - 1 should be in the selection (at some index j)
            // - and all columns in between should also be in the selection
            //   (meaning j-i+1==toIndex-fromIndex, because there are no duplicates in m_cols)
            int i = Arrays.binarySearch(m_cols, fromIndex);
            if (i < 0) {
                return false;
            }
            int j = Arrays.binarySearch(m_cols, i, m_cols.length, toIndex - 1);
            if (j < 0) {
                return false;
            }
            return j - i + 1 == toIndex - fromIndex;
        }
    }

    @Override
    public int[] getSelected() {
        return m_cols;
    }

    @Override
    public int[] getSelected(final int fromIndex, final int toIndex) {
        if (fromIndex > toIndex) {
            throw new IllegalArgumentException("fromIndex(" + fromIndex + ") > toIndex(" + toIndex + ")");
        }
        if (allSelected()) {
            return IntStream.range(fromIndex, toIndex).toArray();
        } else {
            if ( m_cols.length == 0 || (m_cols[0] >= fromIndex && m_cols[m_cols.length-1] < toIndex) ) {
                return m_cols;
            }
            int i = Arrays.binarySearch(m_cols, fromIndex);
            if (i < 0) {
                i = -(i + 1);
            }
            int j = Arrays.binarySearch(m_cols, i, m_cols.length, toIndex);
            if (j < 0) {
                j = -(j + 1);
            }
            return Arrays.copyOfRange(m_cols, i, j);
        }
    }

    @Override
    public ColumnSelection retain(final int... columns) {
        if (columns == null) {
            throw new NullPointerException();
        }

        final int[] cols2 = columns.clone();
        Arrays.sort(cols2);
        if (allSelected()) {
            return new DefaultColumnSelection(removeDuplicates(cols2), false);
        } else {
            int j = 0;
            // merge m_cols and cols2, removing duplicates from cols2 in the process
            for (int i = 0, i2 = 0; i < m_cols.length && i2 < cols2.length; ) {
                if (m_cols[i] == cols2[i2]) {
                    cols2[j] = m_cols[i];
                    ++i;
                    ++i2;
                    ++j;
                } else if (m_cols[i] < cols2[i2]) {
                    ++i;
                } else {
                    ++i2;
                }
            }
            return new DefaultColumnSelection((j < cols2.length) ? Arrays.copyOf(cols2, j) : cols2, false);
        }
    }

    private static int[] removeDuplicates(final int[] sorted) {
        int j = 1;
        for (int i = 1; i < sorted.length; ++i) {
            if (sorted[i] > sorted[j - 1]) {
                sorted[j] = sorted[i];
                ++j;
            }
        }
        return (j < sorted.length) ? Arrays.copyOf(sorted, j) : sorted;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(m_cols);
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ColumnSelection)) {
            return false;
        }
        final ColumnSelection that = (ColumnSelection)obj;
        if (allSelected() != that.allSelected()) {
            return false;
        }
        if (allSelected() && that.allSelected()) {
            return true;
        }
        return Arrays.equals(this.getSelected(), that.getSelected());
    }

    @Override
    public String toString() {
        if (allSelected()) {
            return "select all columns";
        } else {
            return "select columns " + Arrays.toString(m_cols);
        }
    }
}
