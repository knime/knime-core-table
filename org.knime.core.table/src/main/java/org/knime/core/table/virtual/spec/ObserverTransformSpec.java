package org.knime.core.table.virtual.spec;

import java.util.Arrays;

import org.knime.core.table.access.LongAccess;
import org.knime.core.table.access.ReadAccess;

public final class ObserverTransformSpec implements TableTransformSpec {

    /**
     * A {@code ObserverWithRowIndexFactory} creates {@code Observer}s.
     * <p>
     * A listener is created with pre-defined input accesses. Whenever the
     * listener is {@link Observer#update updated}, it reads the current
     * values from the inputs.
     */
    public interface ObserverWithRowIndexFactory {

        interface Observer {
            void update(long rowIndex);
        }

        /**
         * Create an observer with the specified {@code inputs}. Whenever the
         * observer is {@link Observer#update updated}, it reads the current
         * values from the inputs.
         *
         * @param inputs accesses to read input values from
         * @return an observer reading from {@code inputs}.
         */
        Observer createObserver(final ReadAccess[] inputs);
    }

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

        default ObserverWithRowIndexFactory getObserverWithRowIndexFactory() {
            return null;
        }
    }

    public static class WrappedObserverWithRowIndexFactory implements ObserverFactory {

        private ObserverWithRowIndexFactory factory;

        public WrappedObserverWithRowIndexFactory(final ObserverWithRowIndexFactory factory) {
            this.factory = factory;
        }

        @Override
        public Runnable createObserver(ReadAccess[] inputs) {

            // the last input is the rowIndex
            final LongAccess.LongReadAccess rowIndex = (LongAccess.LongReadAccess)inputs[inputs.length - 1];

            // create a ObserverWithRowIndexFactory with the remaining inputs
            final ReadAccess[] inputsWithoutRowIndex = Arrays.copyOf(inputs, inputs.length - 1);
            final ObserverWithRowIndexFactory.Observer observer = factory.createObserver(inputsWithoutRowIndex);

            return () -> observer.update(rowIndex.getLongValue());
        }

        @Override
        public ObserverWithRowIndexFactory getObserverWithRowIndexFactory() {
            return factory;
        }
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
     * Create a {@code ObserverTransformSpec}. This is defined by an array of
     * {@code n} column indices that form the inputs of the observer. The
     * observer is passed the values of the respective columns for each row.
     * <p>
     * The observer is created by a {@code ObserverWithRowIndexFactory} which
     * can be used to create multiple instances of the observer for processing
     * multiple rows in parallel. (Each observer is used single-threaded.)
     * <p>
     * The order in which {@code columnIndices} are given matters. For example
     * if {@code columnIndices = {5,1,4}}, then values from the 5th, 1st, and
     * 4th column are provided as inputs 0, 1, and 2, respectively, to the
     * observer.
     *
     * @param columnIndices the indices of the columns that are passed to the observer
     * @param factory factory to create instances of the observer
     */
    public ObserverTransformSpec(final int[] columnIndices, final ObserverWithRowIndexFactory factory) {
        this.inputColumnIndices = columnIndices;
        this.observerFactory = new WrappedObserverWithRowIndexFactory(factory);
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

    public boolean needsRowIndex() {
        return observerFactory.getObserverWithRowIndexFactory() != null;
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
