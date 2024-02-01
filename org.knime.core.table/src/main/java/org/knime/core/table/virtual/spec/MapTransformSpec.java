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
