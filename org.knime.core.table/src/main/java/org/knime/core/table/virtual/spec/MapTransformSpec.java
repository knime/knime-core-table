package org.knime.core.table.virtual.spec;

import java.util.Arrays;
import java.util.function.BiFunction;

import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.access.WriteAccess;
import org.knime.core.table.schema.ColumnarSchema;

public final class MapTransformSpec implements TableTransformSpec {

    /**
     * A {@code MapperFactory} creates {@code Runnable} mappers.
     * <p>
     * A mapper is created with pre-defined input and output accesses. Whenever the
     * mapper is {@code run()}, it reads the current values from the inputs, computes
     * the map function, and sets the result values to the output accesses.
     */
    public interface MapperFactory {

        /**
         * @return the ColumnarSchema of the columns produced by the map function
         */
        ColumnarSchema getOutputSchema();

        /**
         * Create a mapper with the specified {@code inputs} and {@code outputs}. Whenever
         * the returned mapper is {@code run()}, it reads the current values from the input
         * accesses, computes the map function, and sets the result values to the output
         * accesses.
         *
         * @param inputs  accesses to read input values from
         * @param outputs accesses to write results to
         * @return a mapper reading from {@code inputs} and writing to {@code outputs}.
         */
        Runnable createMapper(final ReadAccess[] inputs, final WriteAccess[] outputs);

        /**
         * Wrap {@code createMapper} as a {@code MapperFactory} with the given
         * output {@code schema}. The BiFunction {@code createMapper} takes an
         * array of input {@code ReadAccess}es and an array of output {@code
         * WriteAccess}es and produces a {@code Runnable} mapper function.
         *
         * @param schema output schema
         * @param createMapper creates {@code Runnable} mappers
         */
        static MapperFactory of( //
                final ColumnarSchema schema, //
                final BiFunction<ReadAccess[], WriteAccess[], ? extends Runnable> createMapper) {
            return new MapTransformUtils.DefaultMapperFactory(schema, createMapper);
        }
    }

    private final int[] inputColumnIndices;

    private final MapperFactory mapperFactory;

    public MapTransformSpec(final int[] columnIndices, final MapperFactory mapperFactory) {
        this.inputColumnIndices = columnIndices;
        this.mapperFactory = mapperFactory;
    }

    /**
     * @return The (input) column indices required for the map computation.
     */
    public int[] getColumnSelection() {
        return inputColumnIndices.clone();
    }

    /**
     * Get the factory used to create mappers. Mappers accept the {@link
     * #getColumnSelection() selected columns} as inputs and produces outputs
     * according to {@link MapperFactory#getOutputSchema()}.
     *
     * @return the MapperFactory
     */
    public MapperFactory getMapperFactory() {
        return mapperFactory;
    }

    @Override
    public String toString() {
        return "Map " + Arrays.toString(inputColumnIndices);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MapTransformSpec that)) {
            return false;
        }

        if (!Arrays.equals(inputColumnIndices, that.inputColumnIndices)) {
            return false;
        }
        return mapperFactory.equals(that.mapperFactory);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(inputColumnIndices);
        result = 31 * result + mapperFactory.hashCode();
        return result;
    }
}
