package org.knime.core.table.virtual.spec;

import java.util.Arrays;
import java.util.function.BooleanSupplier;
import java.util.function.DoublePredicate;
import java.util.function.IntPredicate;

import org.knime.core.table.access.DoubleAccess;
import org.knime.core.table.access.IntAccess;
import org.knime.core.table.access.ReadAccess;

public final class ProgressListenerTransformSpec implements TableTransformSpec {

    /**
     * A {@code ProgressListenerWithRowIndexFactory} creates {@code ProgressListener}s.
     * <p>
     * A listener is created with pre-defined input accesses. Whenever the
     * listener is {@link ProgressListener#update updated}, it reads the current
     * values from the inputs.
     */
    public interface ProgressListenerWithRowIndexFactory {

        interface ProgressListener {
            void update(long rowIndex);
        }

        /**
         * Create a progress listener with the specified {@code inputs}.
         * Whenever the listener is {@link ProgressListener#update updated}, it
         * reads the current values from the inputs.
         *
         * @param inputs accesses to read input values from
         * @return a progress listener reading from {@code inputs}.
         */
        ProgressListener createProgressListener(final ReadAccess[] inputs);
    }

    /**
     * A {@code ProgressListenerFactory} creates {@code Runnable} progress
     * listeners.
     * <p>
     * A listener is created with pre-defined input accesses. Whenever the
     * listener is {@code run()}, it reads the current values from the inputs.
     */
    public interface ProgressListenerFactory {

        /**
         * Create a progress listener with the specified {@code inputs}. Whenever the
         * filter is {@code run()}, it reads the current values from the inputs.
         *
         * @param inputs accesses to read input values from
         * @return a progress listener reading from {@code inputs}.
         */
        Runnable createProgressListener(final ReadAccess[] inputs);
    }

    private final int[] inputColumnIndices;
    private final ProgressListenerFactory progressListenerFactory;

    /**
     * Create a {@code ProgressListenerTransformSpec}. This is defined by an
     * array of {@code n} column indices that form the inputs of the progress
     * listener. The listener is passed on the values of the respective columns
     * for each row.
     * <p>
     * The listener is given by a {@code ProgressListenerFactory} which can be
     * used to create multiple instances of the listener for processing multiple
     * lines in parallel. (Each listener is used single-threaded.)
     * <p>
     * The order in which {@code columnIndices} are given matters. For example
     * if {@code columnIndices = {5,1,4}}, then values from the 5th, 1st, and
     * 4th column are provided as inputs 0, 1, and 2, respectively, to the
     * listener.
     *
     * @param columnIndices the indices of the columns that are passed to the progress listener
     * @param factory factory to create instances of the progress listener
     */
    public ProgressListenerTransformSpec(final int[] columnIndices, final ProgressListenerFactory factory) {
        this.inputColumnIndices = columnIndices;
        this.progressListenerFactory = factory;
    }

    /**
     * @return The (input) column indices required to test the filter predicate.
     */
    public int[] getColumnSelection() {
        return inputColumnIndices.clone();
    }

    public ProgressListenerFactory getProgressListenerFactory() {
        return progressListenerFactory;
    }

    @Override
    public String toString() {
        return "ProgressListener " + Arrays.toString(inputColumnIndices);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ProgressListenerTransformSpec)) {
            return false;
        }

        final ProgressListenerTransformSpec that = (ProgressListenerTransformSpec)o;
        if (!Arrays.equals(inputColumnIndices, that.inputColumnIndices)) {
            return false;
        }
        return progressListenerFactory.equals(that.progressListenerFactory);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(inputColumnIndices);
        result = 31 * result + progressListenerFactory.hashCode();
        return result;
    }
}
