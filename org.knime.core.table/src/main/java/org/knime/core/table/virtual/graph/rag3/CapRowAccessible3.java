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
package org.knime.core.table.virtual.graph.rag3;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.knime.core.table.cursor.Cursor;
import org.knime.core.table.row.DefaultReadAccessRow;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.row.Selection;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.graph.cap.CursorAssemblyPlan;
import org.knime.core.table.virtual.graph.exec.AssembleNodeImps;
import org.knime.core.table.virtual.graph.exec.CapExecutorUtils;
import org.knime.core.table.virtual.graph.exec.SequentialNodeImpConsumer;

class CapRowAccessible3 implements RowAccessible {

    private final CursorAssemblyPlan cap;

    private final ColumnarSchema schema;

    private final Map<UUID, RowAccessible> availableSources;

    CapRowAccessible3( //
            final CursorAssemblyPlan cap, //
            final ColumnarSchema schema, //
            final Map<UUID, RowAccessible> availableSources) {
        this.cap = cap;
        this.schema = schema;
        this.availableSources = availableSources;
    }

    @Override
    public ColumnarSchema getSchema() {
        return schema;
    }

    @Override
    public Cursor<ReadAccessRow> createCursor() {
        return new CapCursor(assembleConsumer(), schema);
    }

    private SequentialNodeImpConsumer assembleConsumer() {
        final List<RowAccessible> sources = CapExecutorUtils.getSources(cap, availableSources);
        return new AssembleNodeImps(cap.nodes(), sources).getConsumer();
    }

    @Override
    public Cursor<ReadAccessRow> createCursor(final Selection selection) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public long size() {
        return cap.numRows();
    }

    @Override
    public void close() throws IOException {
        // TODO ?
    }

    static class CapCursor implements Cursor<ReadAccessRow> {

        final SequentialNodeImpConsumer node;

        private final ReadAccessRow access;

        public CapCursor(final SequentialNodeImpConsumer node, final ColumnarSchema schema) {
            node.create();
            access = new DefaultReadAccessRow(schema.numColumns(), node::getOutput);
            this.node = node;
        }

        @Override
        public ReadAccessRow access() {
            return access;
        }

        @Override
        public boolean forward() {
            return node.forward();
        }

        @Override
        public void close() throws IOException {
            node.close();
        }
    }

}
