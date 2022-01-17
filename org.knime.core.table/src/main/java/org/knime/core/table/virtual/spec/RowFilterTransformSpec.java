package org.knime.core.table.virtual.spec;

import java.util.Arrays;

import org.knime.core.table.access.ReadAccess;

public final class RowFilterTransformSpec implements TableTransformSpec {

    public interface RowFilter {

        /**
         * @return {@code true} if this row should be included in the filtered table, or
         *         {@code false} if it should be filtered out.
         */
        boolean test(final ReadAccess[] inputs);
    }

    private final int[] inputColumnIndices;
    private final RowFilter filter;

    public RowFilterTransformSpec(final int[] columnIndices, final RowFilter filter) {
        this.inputColumnIndices = columnIndices;
        this.filter = filter;
    }

    /**
     * @return The (input) column indices required to test the filter predicate.
     */
    public int[] getColumnSelection() {
        return inputColumnIndices.clone();
    }

    public RowFilter getFilter() {
        return filter;
    }

    @Override
    public String toString() {
        return "RowFilter " + Arrays.toString(inputColumnIndices);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof RowFilterTransformSpec))
            return false;

        final RowFilterTransformSpec that = (RowFilterTransformSpec)o;
        if (!Arrays.equals(inputColumnIndices, that.inputColumnIndices))
            return false;
        return filter.equals(that.filter);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(inputColumnIndices);
        result = 31 * result + filter.hashCode();
        return result;
    }
}
