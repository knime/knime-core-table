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
 *
 * History
 *   Apr 12, 2021 (marcel): created
 */
package org.knime.core.table.virtual;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.cursor.Cursor;
import org.knime.core.table.cursor.Cursors;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.row.Selection;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.spec.AggregateTransformSpec.AggregatorFactory;
import org.knime.core.table.virtual.spec.RowFilterTransformSpec;
import org.knime.core.table.virtual.spec.RowFilterTransformSpec.RowFilterFactory;

/**
 * Implementation of the operation specified by {@link RowFilterTransformSpec}.
 *
 * @author Tobias Pietzsch
 */
class AggregatedRowAccessible implements RowAccessible {

    private final RowAccessible m_delegateTable;

    private final int[] m_inputs;

    private final Aggregator<?, ?> aggregator;

    public AggregatedRowAccessible(final RowAccessible tableToFilter, final int[] inputColumns, final AggregatorFactory<?> aggregatorFactory) {
        m_delegateTable = tableToFilter;
        m_inputs = inputColumns;
        aggregator = new Aggregator<>(aggregatorFactory);
    }

    private class Aggregator<A, T extends RowAccessible & Consumer<A>>
    {
        private final AggregatorFactory<A> factory;
        private final T result;

        Aggregator(AggregatorFactory<A> factory) {
            this.factory = factory;
            result = factory.finisher();
        }

        public RowAccessible result()
        {
            return result;
        }

        public void aggregate()
        {
            try(var cursor = m_delegateTable.createCursor()) {
                ReadAccess[] inputs = new ReadAccess[m_inputs.length];
                Arrays.setAll(inputs, i -> cursor.access().getAccess(m_inputs[i]));
                Consumer<A> accumulator = factory.accumulator(inputs);
                A container = factory.supplier().get();
                while (cursor.forward())
                    accumulator.accept(container);
                result.accept(container);
            } catch (IOException e) {
                throw new RuntimeException(e); // TODO
            }
        }
    }

    @Override
    public ColumnarSchema getSchema() {
        return aggregator.result().getSchema();
    }

    @Override
    public Cursor<ReadAccessRow> createCursor() {
        // TODO: Aggregate only once. (When the first Cursor is created)
        // TODO: Ideally we probably want to postpone aggregate to the first
        //       forward() of the first created Cursor This is just the reference
        //       implementation, so it's probably not crucial.
        //       GraphVirtualTableExecutor should do it, however.
        aggregator.aggregate();
        return aggregator.result().createCursor();
    }

    @Override
    public Cursor<ReadAccessRow> createCursor(final Selection selection) {
        // TODO: Aggregate only once. (When the first Cursor is created)
        // TODO: Ideally we probably want to postpone aggregate to the first
        //       forward() of the first created Cursor This is just the reference
        //       implementation, so it's probably not crucial.
        //       GraphVirtualTableExecutor should do it, however.
        aggregator.aggregate();
        return aggregator.result().createCursor(selection);
    }

    /**
     * Returns {@code -1}, because it is unknown how many rows will be filtered out.
     *
     * @return {@code -1}, indicating that the number of rows is unknown.
     */
    @Override
    public long size() {
        return aggregator.result().size();
    }

    @Override
    public void close() throws IOException {
        m_delegateTable.close();
    }
}
