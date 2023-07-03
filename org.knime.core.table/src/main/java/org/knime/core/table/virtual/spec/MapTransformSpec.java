package org.knime.core.table.virtual.spec;

import static org.knime.core.table.schema.DataSpecs.DOUBLE;
import static org.knime.core.table.schema.DataSpecs.INT;

import java.util.Arrays;
import java.util.function.BiFunction;
import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleUnaryOperator;
import java.util.function.IntBinaryOperator;
import java.util.function.IntUnaryOperator;

import org.knime.core.table.access.DoubleAccess;
import org.knime.core.table.access.IntAccess;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.access.WriteAccess;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.spec.MapTransformSpec.MapperWithRowIndexFactory.Mapper;

public final class MapTransformSpec implements TableTransformSpec {

    /**
     * A {@code MapperWithRowIndexFactory} creates {@code Mapper}s.
     * <p>
     * A mapper is created with pre-defined input and output accesses. Whenever
     * {@code Mapper.map(rowIndex)} is called, it reads the current values from
     * the inputs, computes the map function, and sets the result values to the
     * output accesses.
     */
    public interface MapperWithRowIndexFactory {

        interface Mapper {
            void map(long rowIndex);
        }

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
        Mapper createMapper(final ReadAccess[] inputs, final WriteAccess[] outputs);
    }

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

        default MapperWithRowIndexFactory getMapperWithRowIndexFactory() {
            return null;
        }

        static MapperFactory doublesToDouble(final DoubleUnaryOperator fn) {
            return new DefaultMapperFactory(ColumnarSchema.of(DOUBLE), //
                    (inputs, outputs) -> {
                        verify(inputs, 1, outputs, 1);
                        final DoubleAccess.DoubleReadAccess i = (DoubleAccess.DoubleReadAccess)inputs[0];
                        final DoubleAccess.DoubleWriteAccess o = (DoubleAccess.DoubleWriteAccess)outputs[0];
                        return () -> o.setDoubleValue(fn.applyAsDouble(i.getDoubleValue()));
                    });
        }

        static MapperFactory doublesToDouble(final DoubleBinaryOperator fn) {
            return new DefaultMapperFactory(ColumnarSchema.of(DOUBLE), //
                    (inputs, outputs) -> {
                        verify(inputs, 2, outputs, 1);
                        final DoubleAccess.DoubleReadAccess i0 = (DoubleAccess.DoubleReadAccess)inputs[0];
                        final DoubleAccess.DoubleReadAccess i1 = (DoubleAccess.DoubleReadAccess)inputs[1];
                        final DoubleAccess.DoubleWriteAccess o = (DoubleAccess.DoubleWriteAccess)outputs[0];
                        return () -> o.setDoubleValue(fn.applyAsDouble(i0.getDoubleValue(), i1.getDoubleValue()));
                    });
        }

        static MapperFactory intsToInt(final IntUnaryOperator fn) {
            return new DefaultMapperFactory(ColumnarSchema.of(INT), //
                    (inputs, outputs) -> {
                        verify(inputs, 1, outputs, 1);
                        final IntAccess.IntReadAccess i = (IntAccess.IntReadAccess)inputs[0];
                        final IntAccess.IntWriteAccess o = (IntAccess.IntWriteAccess)outputs[0];
                        return () -> o.setIntValue(fn.applyAsInt(i.getIntValue()));
                    });
        }

        static MapperFactory intsToInt(final IntBinaryOperator fn) {
            return new DefaultMapperFactory(ColumnarSchema.of(INT), //
                    (inputs, outputs) -> {
                        verify(inputs, 2, outputs, 1);
                        final IntAccess.IntReadAccess i0 = (IntAccess.IntReadAccess)inputs[0];
                        final IntAccess.IntReadAccess i1 = (IntAccess.IntReadAccess)inputs[1];
                        final IntAccess.IntWriteAccess o = (IntAccess.IntWriteAccess)outputs[0];
                        return () -> o.setIntValue(fn.applyAsInt(i0.getIntValue(), i1.getIntValue()));
                    });
        }

        static void verify(final ReadAccess[] inputs, final int expectedNumInputs, final WriteAccess[] outputs,
                final int expectedNumOutputs) {
            if (inputs == null || outputs == null) {
                throw new NullPointerException();
            }
            if (inputs.length != expectedNumInputs) {
                throw new IllegalArgumentException(
                        "expected " + expectedNumInputs + " inputs (instead of " + inputs.length + ")");
            }
            if (outputs.length != expectedNumOutputs) {
                throw new IllegalArgumentException(
                        "expected " + expectedNumOutputs + " outputs (instead of " + outputs.length + ")");
            }
        }
    }

    public static class DefaultMapperFactory implements MapperFactory {

        private final ColumnarSchema schema;

        private final BiFunction<ReadAccess[], WriteAccess[], Runnable> createMapper;

        public DefaultMapperFactory(final ColumnarSchema schema,
                final BiFunction<ReadAccess[], WriteAccess[], Runnable> createMapper) {
            this.schema = schema;
            this.createMapper = createMapper;
        }

        @Override
        public ColumnarSchema getOutputSchema() {
            return schema;
        }

        @Override
        public Runnable createMapper(final ReadAccess[] inputs, final WriteAccess[] outputs) {
            return createMapper.apply(inputs, outputs);
        }
    }

    public static class WrappedMapperWithRowIndexFactory implements MapperFactory {

        private MapperWithRowIndexFactory factory;

        public WrappedMapperWithRowIndexFactory(final MapperWithRowIndexFactory factory) {
            this.factory = factory;
        }

        @Override
        public ColumnarSchema getOutputSchema() {
            return factory.getOutputSchema();
        }

        @Override
        public Runnable createMapper(ReadAccess[] inputs, WriteAccess[] outputs) {
            final Mapper mapper = factory.createMapper(inputs, outputs);
            return new Runnable() {
                private long m_rowIndex = 0;

                @Override
                public void run() {
                    mapper.map(m_rowIndex);
                    m_rowIndex++;
                }
            };
        }

        @Override
        public MapperWithRowIndexFactory getMapperWithRowIndexFactory() {
            return factory;
        }
    }

    private final int[] inputColumnIndices;

    private final MapperFactory mapperFactory;

    public MapTransformSpec(final int[] columnIndices, final MapperFactory mapperFactory) {
        this.inputColumnIndices = columnIndices;
        this.mapperFactory = mapperFactory;
    }

    public MapTransformSpec(final int[] columnIndices, final MapperWithRowIndexFactory mapperFactory) {
        this.inputColumnIndices = columnIndices;
        this.mapperFactory = new WrappedMapperWithRowIndexFactory(mapperFactory);
    }

    /**
     * @return The (input) column indices required for the map computation.
     */
    public int[] getColumnSelection() {
        return inputColumnIndices.clone();
    }

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
        if (!(o instanceof MapTransformSpec)) {
            return false;
        }

        final MapTransformSpec that = (MapTransformSpec)o;
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
