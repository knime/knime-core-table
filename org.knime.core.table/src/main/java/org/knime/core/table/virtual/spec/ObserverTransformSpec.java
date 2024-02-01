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
        if (!(o instanceof ObserverTransformSpec that)) {
            return false;
        }

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
