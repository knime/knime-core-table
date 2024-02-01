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
