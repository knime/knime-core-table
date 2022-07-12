package org.knime.core.table.virtual.spec;

import java.util.Arrays;
import java.util.function.BooleanSupplier;

import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.access.WriteAccess;

public final class RowFilterTransformSpec implements TableTransformSpec {

    /**
     * A {@code RowFilterFactory} creates {@code BooleanSupplier} row filters.
     * <p>
     * A row filter is created with pre-defined input accesses. Whenever the
     * filter is {@link BooleanSupplier#getAsBoolean run}, it reads the current
     * values from the inputs and returns {@code true} if the row passes the
     * filter.
     */
    public interface RowFilterFactory {

        /**
         * Create a row filter with the specified {@code inputs}. Whenever the
         * returned filter is {@link BooleanSupplier#getAsBoolean run}, it reads
         * the current values from the inputs. The filter returns {@code true}
         * if the current row should be included in the filtered table, or
         * {@code false} if it should be filtered out.
         *
         * @param inputs  accesses to read input values from
         * @return a row filter reading from {@code inputs}.
         */
        BooleanSupplier createRowFilter(final ReadAccess[] inputs);
    }

    private final int[] inputColumnIndices;
    private final RowFilterFactory filterFactory;

    public RowFilterTransformSpec(final int[] columnIndices, final RowFilterFactory filterFactory) {
        this.inputColumnIndices = columnIndices;
        this.filterFactory = filterFactory;
    }

    /**
     * @return The (input) column indices required to test the filter predicate.
     */
    public int[] getColumnSelection() {
        return inputColumnIndices.clone();
    }

    public RowFilterFactory getFilterFactory() {
        return filterFactory;
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
        if (!Arrays.equals(inputColumnIndices, that.inputColumnIndices)) {
            return false;
        }
        return filterFactory.equals(that.filterFactory);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(inputColumnIndices);
        result = 31 * result + filterFactory.hashCode();
        return result;
    }
}
