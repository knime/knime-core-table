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
package org.knime.core.table.virtual.graph.exec;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;

import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.row.RowWriteAccessible;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.graph.cap.CapBuilder;
import org.knime.core.table.virtual.graph.cap.CursorAssemblyPlan;
import org.knime.core.table.virtual.graph.rag.RagBuilder;
import org.knime.core.table.virtual.graph.rag.RagGraph;
import org.knime.core.table.virtual.graph.rag.RagNode;
import org.knime.core.table.virtual.spec.SourceTableProperties.CursorType;

public class CapExecutor {

    /**
     * @param specGraph
     * @param schema
     * @param cursorType
     * @param uuidRowAccessibleMap
     * @return
     */
    public static RowAccessible createRowAccessible(
            final RagGraph specGraph,
            final ColumnarSchema schema,
            final CursorType cursorType,
            final Map<UUID, RowAccessible> uuidRowAccessibleMap) {
        return switch (cursorType) {
            case BASIC -> new CapRowAccessible(specGraph, schema, uuidRowAccessibleMap);
            case LOOKAHEAD -> new CapLookaheadRowAccessible(specGraph, schema, uuidRowAccessibleMap);
            case RANDOMACCESS -> new CapRandomRowAccessible(specGraph, schema, uuidRowAccessibleMap);
        };
    }

    public static void execute(
            final RagGraph specGraph,
            final Map<UUID, RowAccessible> uuidRowAccessibleMap, //
            final Map<UUID, RowWriteAccessible> uuidRowWriteAccessibleMap //
    ) throws CompletionException, CancellationException {

        try {
            final List<RagNode> orderedRag = RagBuilder.createOrderedRag(specGraph);
            final CursorAssemblyPlan cap = CapBuilder.createCursorAssemblyPlan(orderedRag);
            final List<RowAccessible> sources = CapExecutorUtils.getSources(cap, uuidRowAccessibleMap);
            final List<RowWriteAccessible> sinks = CapExecutorUtils.getSinks(cap, uuidRowWriteAccessibleMap);

            final SequentialNodeImp terminator = new AssembleNodeImps(cap.nodes(), sources, sinks).getTerminator();
            terminator.create();
            terminator.forward();
            terminator.close();
        } catch (CancellationException | CompletionException ex) {
            throw ex;
        } catch (Throwable ex) {
            throw new CompletionException(ex);
        }
    }
}
