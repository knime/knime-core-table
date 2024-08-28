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
package org.knime.core.table.virtual.exec;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;

import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.row.RowWriteAccessible;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.TableTransform;
import org.knime.core.table.virtual.graph.exec.CapExecutor;
import org.knime.core.table.virtual.graph.rag3.TableTransformGraph;
import org.knime.core.table.virtual.graph.rag3.TableTransformUtil;
import org.knime.core.table.virtual.spec.SourceTableProperties.CursorType;

public class GraphVirtualTableExecutor implements VirtualTableExecutor {

    private final TableTransformGraph tableTransformGraph;
    private final ColumnarSchema schema;
    private final CursorType cursorType;

    public GraphVirtualTableExecutor(final TableTransform leafTransform)
    {
        tableTransformGraph = new TableTransformGraph(leafTransform);
        schema = TableTransformUtil.createSchema(tableTransformGraph);
        cursorType = tableTransformGraph.supportedCursorType();
    }

    @Override
    public List<RowAccessible> execute(Map<UUID, RowAccessible> inputs) {
        final RowAccessible rows = CapExecutor.createRowAccessible(tableTransformGraph, schema, cursorType, inputs);
        return List.of(rows);
    }

    /**
     * Run the comp graph, reading from the provided {@code inputs} and writing
     * to the provided {@code outputs}.
     *
     * @param inputs
     * @param outputs
     * @throws CancellationException if the computation was cancelled
     * @throws CompletionException if the computation threw an exception
     */
    public void execute( // TODO (TP): REMOVE
            Map<UUID, RowAccessible> inputs,//
            Map<UUID, RowWriteAccessible> outputs//
    ) throws CancellationException, CompletionException {
        throw new UnsupportedOperationException("TODO (TP): REMOVE");
    }
}
