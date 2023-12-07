package org.knime.core.table.virtual.spec;

import java.util.Arrays;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.access.WriteAccess;
import org.knime.core.table.schema.ColumnarSchema;

public final class ObserverTransformSpec implements TableTransformSpec {

    /**
     * A {@code ObserverFactory} creates {@code Runnable} observers.
     * <p>
     * A listener is created with pre-defined input accesses. Whenever the
     * listener is {@code run()}, it reads the current values from the inputs.
     */
    public interface ObserverFactory {

        /**
         * Create an observer with the specified {@code inputs}. Whenever the
         * filter is {@code run()}, it reads the current values from the inputs.
         *
         * @param inputs accesses to read input values from
         * @return an observer reading from {@code inputs}.
         */
        Runnable createObserver(final ReadAccess[] inputs);
    }

    private final int[] inputColumnIndices;

    private final ObserverFactory observerFactory;

    /**
     * Create a {@code ObserverTransformSpec}. This is defined by an array of
     * {@code n} column indices that form the inputs of the observer. The
     * observer is passed the values of the respective columns for each row.
     * <p>
     * The observer is created by a {@code ObserverFactory} which can be
     * used to create multiple instances of the observer for processing multiple
     * rows in parallel. (Each observer is used single-threaded.)
     * <p>
     * The order in which {@code columnIndices} are given matters. For example
     * if {@code columnIndices = {5,1,4}}, then values from the 5th, 1st, and
     * 4th column are provided as inputs 0, 1, and 2, respectively, to the
     * observer.
     *
     * @param columnIndices the indices of the columns that are passed to the observer
     * @param factory factory to create instances of the observer
     */
    public ObserverTransformSpec(final int[] columnIndices, final ObserverFactory factory) {
        this.inputColumnIndices = columnIndices;
        this.observerFactory = factory;
    }

    /**
     * @return The (input) column indices required to test the filter predicate.
     */
    public int[] getColumnSelection() {
        return inputColumnIndices.clone();
    }

    public ObserverFactory getObserverFactory() {
        return observerFactory;
    }

    @Override
    public String toString() {
        return "Observer " + Arrays.toString(inputColumnIndices);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ObserverTransformSpec)) {
            return false;
        }

        final ObserverTransformSpec that = (ObserverTransformSpec)o;
        if (!Arrays.equals(inputColumnIndices, that.inputColumnIndices)) {
            return false;
        }
        return observerFactory.equals(that.observerFactory);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(inputColumnIndices);
        result = 31 * result + observerFactory.hashCode();
        return result;
    }
}
