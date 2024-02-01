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
import java.util.function.BooleanSupplier;
import java.util.function.DoublePredicate;
import java.util.function.IntPredicate;

import org.knime.core.table.access.DoubleAccess;
import org.knime.core.table.access.IntAccess;
import org.knime.core.table.access.ReadAccess;

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

        static RowFilterFactory intPredicate(final IntPredicate predicate) {
            return inputs -> {
                verify(inputs, 1);
                final IntAccess.IntReadAccess i0 = (IntAccess.IntReadAccess)inputs[0];
                return () -> predicate.test(i0.getIntValue());
            };
        }

        static RowFilterFactory doublePredicate(final DoublePredicate predicate) {
            return inputs -> {
                verify(inputs, 1);
                final DoubleAccess.DoubleReadAccess i0 = (DoubleAccess.DoubleReadAccess)inputs[0];
                return () -> predicate.test(i0.getDoubleValue());
            };
        }

        private static void verify(final ReadAccess[] inputs, final int expectedNumInputs) {
            if (inputs == null) {
                throw new NullPointerException();
            }
            if (inputs.length != expectedNumInputs) {
                throw new IllegalArgumentException(
                        "expected " + expectedNumInputs + " inputs (instead of " + inputs.length + ")");
            }
        }
    }

    private final int[] inputColumnIndices;
    private final RowFilterFactory filterFactory;

    /**
     * Create a {@code RowFilterTransformSpec}. This is defined by an array of
     * {@code n} column indices that form the inputs of the ({@code n}-ary}
     * filter predicate. The predicate is evaluated on the values of the
     * respective columns for each row. Rows for which the predicate evaluates
     * to {@code true} will be included in the resulting {@code VirtualTable},
     * rows for which the filter predicate evaluates to {@code false} will be
     * removed (skipped).
     * <p>
     * The filter is given by a {@code RowFilterFactory} which can be used to
     * create multiple instances of the filter predicate for processing multiple
     * lines in parallel. (Each filter predicate is used single-threaded.)
     * <p>
     * The order in which {@code columnIndices} are given matters. For example
     * if {@code columnIndices = {5,1,4}}, then values from the 5th, 1st, and
     * 4th column are provided as inputs 0, 1, and 2, respectively, to the
     * filter predicate.
     *
     * @param columnIndices the indices of the columns that are passed to the filter predicate
     * @param filterFactory factory to create instances of the filter predicate
     */
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
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RowFilterTransformSpec that)) {
            return false;
        }

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
