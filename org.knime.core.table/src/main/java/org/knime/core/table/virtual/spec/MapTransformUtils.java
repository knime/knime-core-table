/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 */
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
import org.knime.core.table.access.LongAccess;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.access.WriteAccess;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.spec.MapTransformSpec.MapperFactory;

/**
 * Helpers for constructing {@link MapperFactory MapperFactories}.
 */
public class MapTransformUtils {

    /**
     * Verify that {@code inputs} and {@code outputs} arrays are non-{@code
     * null} and have the expected lengths.
     *
     * @param inputs array of input {@code ReadAccess}es
     * @param expectedNumInputs expected number of input {@code ReadAccess}es
     * @param outputs array of output {@code WriteAccess}es
     * @param expectedNumOutputs expected number of output {@code WriteAccess}es
     */
    public static void verify(final ReadAccess[] inputs, final int expectedNumInputs, final WriteAccess[] outputs,
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

    /**
     * Create a {@code MapperFactory} that creates mappers implementing the
     * given {@code DoubleUnaryOperator}.
     *
     * @param fn the operator to wrap
     * @return factory for mappers implementing {@code fn}
     */
    public static MapperFactory doublesToDouble(final DoubleUnaryOperator fn) {
        return MapperFactory.of(ColumnarSchema.of(DOUBLE), //
                (inputs, outputs) -> {
                    verify(inputs, 1, outputs, 1);
                    final DoubleAccess.DoubleReadAccess i = (DoubleAccess.DoubleReadAccess)inputs[0];
                    final DoubleAccess.DoubleWriteAccess o = (DoubleAccess.DoubleWriteAccess)outputs[0];
                    return () -> o.setDoubleValue(fn.applyAsDouble(i.getDoubleValue()));
                });
    }

    /**
     * Create a {@code MapperFactory} that creates mappers implementing the
     * given {@code DoubleBinaryOperator}.
     *
     * @param fn the operator to wrap
     * @return factory for mappers implementing {@code fn}
     */
    public static MapperFactory doublesToDouble(final DoubleBinaryOperator fn) {
        return MapperFactory.of(ColumnarSchema.of(DOUBLE), //
                (inputs, outputs) -> {
                    verify(inputs, 2, outputs, 1);
                    final DoubleAccess.DoubleReadAccess i0 = (DoubleAccess.DoubleReadAccess)inputs[0];
                    final DoubleAccess.DoubleReadAccess i1 = (DoubleAccess.DoubleReadAccess)inputs[1];
                    final DoubleAccess.DoubleWriteAccess o = (DoubleAccess.DoubleWriteAccess)outputs[0];
                    return () -> o.setDoubleValue(fn.applyAsDouble(i0.getDoubleValue(), i1.getDoubleValue()));
                });
    }

    /**
     * Create a {@code MapperFactory} that creates mappers implementing the
     * given {@code IntUnaryOperator}.
     *
     * @param fn the operator to wrap
     * @return factory for mappers implementing {@code fn}
     */
    public static MapperFactory intsToInt(final IntUnaryOperator fn) {
        return MapperFactory.of(ColumnarSchema.of(INT), //
                (inputs, outputs) -> {
                    verify(inputs, 1, outputs, 1);
                    final IntAccess.IntReadAccess i = (IntAccess.IntReadAccess)inputs[0];
                    final IntAccess.IntWriteAccess o = (IntAccess.IntWriteAccess)outputs[0];
                    return () -> o.setIntValue(fn.applyAsInt(i.getIntValue()));
                });
    }

    /**
     * Create a {@code MapperFactory} that creates mappers implementing the
     * given {@code IntBinaryOperator}.
     *
     * @param fn the operator to wrap
     * @return factory for mappers implementing {@code fn}
     */
    public static MapperFactory intsToInt(final IntBinaryOperator fn) {
        return MapperFactory.of(ColumnarSchema.of(INT), //
                (inputs, outputs) -> {
                    verify(inputs, 2, outputs, 1);
                    final IntAccess.IntReadAccess i0 = (IntAccess.IntReadAccess)inputs[0];
                    final IntAccess.IntReadAccess i1 = (IntAccess.IntReadAccess)inputs[1];
                    final IntAccess.IntWriteAccess o = (IntAccess.IntWriteAccess)outputs[0];
                    return () -> o.setIntValue(fn.applyAsInt(i0.getIntValue(), i1.getIntValue()));
                });
    }

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
         * Create a mapper with the specified {@code inputs} and {@code outputs}.
         * Whenever {@code Mapper.map(rowIndex)} is called, the returned mapper reads
         * the current values from the input accesses, computes the map function, and
         * sets the result values to the output accesses.
         *
         * @param inputs  accesses to read input values from
         * @param outputs accesses to write results to
         * @return a mapper reading from {@code inputs} and writing to {@code outputs}.
         */
        Mapper createMapper(final ReadAccess[] inputs, final WriteAccess[] outputs);

        /**
         * Wrap {@code createMapper} as a {@code MapperWithRowIndexFactory} with
         * the given output {@code schema}. The BiFunction {@code createMapper}
         * takes an array of input {@code ReadAccess}es and an array of output
         * {@code WriteAccess}es and produces a {@link Mapper} function.
         *
         * @param schema output schema
         * @param createMapper creates {@link Mapper}s
         */
        static MapperWithRowIndexFactory of( //
                final ColumnarSchema schema, //
                final BiFunction<ReadAccess[], WriteAccess[], Mapper> createMapper) {
            return new DefaultMapperWithRowIndexFactory(schema, createMapper);
        }
    }

    /**
     * Simple {@code MapperFactory} implementation that can be constructed with
     * a {@link BiFunction} lambda.
     */
    static class DefaultMapperFactory implements MapperFactory {

        private final ColumnarSchema schema;

        private final BiFunction<ReadAccess[], WriteAccess[], ? extends Runnable> createMapper;

        /**
         * Wrap {@code createMapper} as a {@code MapperFactory} with the given
         * output {@code schema}. The BiFunction {@code createMapper} takes an
         * array of input {@code ReadAccess}es and an array of output {@code
         * WriteAccess}es and produces a {@code Runnable} mapper function.
         *
         * @param schema output schema
         * @param createMapper creates {@code Runnable} mappers
         */
        DefaultMapperFactory(final ColumnarSchema schema,
                final BiFunction<ReadAccess[], WriteAccess[], ? extends Runnable> createMapper) {
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

    /**
     * Simple {@code MapperWithRowIndexFactory} implementation that can be
     * constructed with a {@link BiFunction} lambda.
     */
    static class DefaultMapperWithRowIndexFactory implements MapperWithRowIndexFactory {

        private final ColumnarSchema schema;

        private final BiFunction<ReadAccess[], WriteAccess[], ? extends Mapper> createMapper;

        /**
         * Wrap {@code createMapper} as a {@code MapperWithRowIndexFactory} with
         * the given output {@code schema}. The BiFunction {@code createMapper}
         * takes an array of input {@code ReadAccess}es and an array of output
         * {@code WriteAccess}es and produces a {@link Mapper} function.
         *
         * @param schema output schema
         * @param createMapper creates {@link Mapper}s
         */
        DefaultMapperWithRowIndexFactory(final ColumnarSchema schema,
                final BiFunction<ReadAccess[], WriteAccess[], ? extends Mapper> createMapper) {
            this.schema = schema;
            this.createMapper = createMapper;
        }

        @Override
        public ColumnarSchema getOutputSchema() {
            return schema;
        }

        @Override
        public Mapper createMapper(final ReadAccess[] inputs, final WriteAccess[] outputs) {
            return createMapper.apply(inputs, outputs);
        }
    }

    /**
     * A {@code MapperFactory} implementation that wraps a {@code
     * MapperWithRowIndexFactory}.
     * <p>
     * Mappers created by this factory have one additional {@code
     * LongReadAccess} input wrt mappers created by the wrapped {@code
     * MapperWithRowIndexFactory}. This additional input represents the row index.
     * When the mapper is {@code run()}, this input is stripped off and passed
     * as the rowIndex argument to the wrapped {@link MapperWithRowIndexFactory.Mapper#map(long)}.
     */
    public static class WrappedMapperWithRowIndexFactory implements MapperFactory {

        private final MapperWithRowIndexFactory factory;

        /**
         * Wrap the given {@code MapperWithRowIndexFactory} as a simple {@code
         * MapperFactory} with the row index appended as an additional input
         * {@code LongReadAccess}.
         */
        public WrappedMapperWithRowIndexFactory(final MapperWithRowIndexFactory factory) {
            this.factory = factory;
        }

        @Override
        public ColumnarSchema getOutputSchema() {
            return factory.getOutputSchema();
        }

        @Override
        public Runnable createMapper(ReadAccess[] inputs, WriteAccess[] outputs) {

            // the last input is the rowIndex
            final LongAccess.LongReadAccess rowIndex = (LongAccess.LongReadAccess)inputs[inputs.length - 1];

            // create a MapperWithRowIndex with the remaining inputs
            final ReadAccess[] inputsWithoutRowIndex = Arrays.copyOf(inputs, inputs.length - 1);
            final MapperWithRowIndexFactory.Mapper
                    mapper = factory.createMapper(inputsWithoutRowIndex, outputs);

            return () -> mapper.map(rowIndex.getLongValue());
        }

        public MapperWithRowIndexFactory getMapperWithRowIndexFactory() {
            return factory;
        }
    }
}
