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
    public int[] getSelected() {
        return m_cols;
    }

    @Override
    public int[] getSelected(int fromIndex, int toIndex) {
        if (fromIndex > toIndex) {
            throw new IllegalArgumentException("fromIndex(" + fromIndex + ") > toIndex(" + toIndex + ")");
        }
        if (allSelected()) {
            return IntStream.range(fromIndex, toIndex).toArray();
        } else {
            if ( m_cols.length == 0 || (m_cols[0] >= fromIndex && m_cols[m_cols.length-1] < toIndex) )
                return m_cols;
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
    public ColumnSelection retain(int... columns) {
        if (columns == null)
            throw new NullPointerException();

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
