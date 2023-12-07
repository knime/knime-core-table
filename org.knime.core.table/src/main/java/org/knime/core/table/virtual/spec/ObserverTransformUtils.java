package org.knime.core.table.virtual.spec;

import java.util.Arrays;

import org.knime.core.table.access.LongAccess;
import org.knime.core.table.access.ReadAccess;

/**
 * Helpers for constructing {@link ObserverTransformSpec.ObserverFactory ObserverFactories}.
 */
public class ObserverTransformUtils {

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

    public static class WrappedObserverWithRowIndexFactory implements ObserverTransformSpec.ObserverFactory {

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

        public ObserverWithRowIndexFactory getObserverWithRowIndexFactory() {
            return factory;
        }
    }
}
