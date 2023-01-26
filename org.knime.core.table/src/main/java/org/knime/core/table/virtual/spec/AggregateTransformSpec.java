package org.knime.core.table.virtual.spec;

import java.util.Arrays;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.schema.ColumnarSchema;

public final class AggregateTransformSpec implements TableTransformSpec {

    /**
     * {@code AggregatorFactory} is modeled after {@link
     * java.util.stream.Collector Collector<T,A,R>}. However, the {@code T} and
     * {@code R} types are replaced by {@code ReadAccess}es supplied to {@code
     * accumulator()} and produced by {@code finisher()}, respectively.
     * <p>
     * An {@code accumulator()} is created with pre-defined input accesses.
     * Whenever the accumulator {@code accept()}s a result container, it reads
     * the current values from the inputs and updates the result container.
     * <p>
     * TODO: more javadoc
     */
    public interface AggregatorFactory<A> {

        Supplier<A> supplier();

        // N.B.
        // MapperFactory.createMapper(ReadAccess[] inputs, WriteAccess[] outputs) is given WriteAccess[] outputs too
        // AggregatorFactory is eventually planning to provide a RowAccessible as the output ???
        // This seems to be a conceptual mismatch. Try to resolve this, as the API evolves...

        // accumulator.accept(A) will read inputs and accumulate into A
        Consumer<A> accumulator(ReadAccess[] inputs);

        // TODO: Add combiner()
        //   A function that accepts two partial results and merges them.
        //   BinaryOperator<A> combiner();
        //   (See java.util.stream.Collector)

        // finisher.accept(A) makes a result container available as a RowAccessible
        // with schema #getOutputSchema()
        <T extends RowAccessible & Consumer<A>> T finisher();

        /**
         * @return the ColumnarSchema of the columns produced by the map function
         */
        ColumnarSchema getOutputSchema();
    }

    private final int[] inputColumnIndices;

    private final AggregatorFactory aggregatorFactory;

    public AggregateTransformSpec(final int[] columnIndices, final AggregatorFactory aggregatorFactory) {
        this.inputColumnIndices = columnIndices;
        this.aggregatorFactory = aggregatorFactory;
    }

    /**
     * @return The (input) column indices required for the map computation.
     */
    public int[] getColumnSelection() {
        return inputColumnIndices.clone();
    }

    public AggregatorFactory getAggregatorFactory() {
        return aggregatorFactory;
    }

    @Override
    public String toString() {
        return "Aggregate " + Arrays.toString(inputColumnIndices);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AggregateTransformSpec)) {
            return false;
        }

        final AggregateTransformSpec that = (AggregateTransformSpec)o;
        if (!Arrays.equals(inputColumnIndices, that.inputColumnIndices)) {
            return false;
        }
        return aggregatorFactory.equals(that.aggregatorFactory);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(inputColumnIndices);
        result = 31 * result + aggregatorFactory.hashCode();
        return result;
    }
}
