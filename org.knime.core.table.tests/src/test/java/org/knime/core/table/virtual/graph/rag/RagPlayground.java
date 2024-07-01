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
package org.knime.core.table.virtual.graph.rag;

import static org.knime.core.table.virtual.graph.rag.RagEdgeType.FLATTENED_ORDER;

import java.util.List;

import org.knime.core.table.virtual.VirtualTable;
import org.knime.core.table.virtual.graph.VirtualTableTests;
import org.knime.core.table.virtual.graph.debug.Mermaid;

public class RagPlayground {

    public static void main(final String[] args) {
//        final VirtualTable table = VirtualTableTests.vtMinimal();
//        final VirtualTable table = VirtualTableTests.vtLinear();
//        final VirtualTable table = VirtualTableTests.vtAppend();
//        final VirtualTable table = VirtualTableTests.vtAppendAndSlice();
//        final VirtualTable table = VirtualTableTests.vtForkJoin();
//        final VirtualTable table = VirtualTableTests.vtForkJoinLookALike();
//        final VirtualTable table = VirtualTableTests.vtConcatenate();
//        final VirtualTable table = VirtualTableTests.vtConcatenateAndSlice();
//        final VirtualTable table = VirtualTableTests.vtConcatenateAndSliceSingleTable();
//        final VirtualTable table = VirtualTableTests.vtConcatenateAndSliceFullSingleTable();
//        final VirtualTable table = VirtualTableTests.vtConcatenateAndSliceFullTable();
//        final VirtualTable table = VirtualTableTests.vtAppendMissing();
//        final VirtualTable table = VirtualTableTests.vtSimpleMap();
//        final VirtualTable table = VirtualTableTests.vtSimpleRowFilter();
//        final VirtualTable table = VirtualTableTests.vtConsecutiveRowFilters();
//        final VirtualTable table = VirtualTableTests.vtMapsAndFilters();
//        final VirtualTable table = VirtualTableTests.vtFiltersMapAndConcatenate();
//        final VirtualTable table = VirtualTableTests.vtMaterializeMinimal();
//        final VirtualTable table = VirtualTableTests.vtRowIndexMap();
//        final VirtualTable table = VirtualTableTests.vtRowIndexMapsParallel();
//        final VirtualTable table = VirtualTableTests.vtRowIndexMapsParallelAndSlice();
//        final VirtualTable table = VirtualTableTests.vtRowIndexMapsSequential();
//        final VirtualTable table = VirtualTableTests.vtRowIndexMapsSequentialAndSlice();
        final VirtualTable table = VirtualTableTests.vtObserve();

        var mermaid = new Mermaid();
        var spec =  new SpecGraphBuilder();

        spec.buildSpec(table.getProducingTransform());
        mermaid.append("<pre>" + spec.graph + "</pre>");
        mermaid.append("buildSpec(table)", "SPEC edges", spec.graph);

        var rag = new RagBuilder(spec.graph);

        rag.traceExec();
        rag.traceAccesses();
        mermaid.append("traceExec(); traceAccesses()", "adds DATA, EXEC edges",  rag.graph);

        rag.optimize();
        mermaid.append("optimize()", rag.graph);

        rag.createExecutionOrderingEdges();
        mermaid.append("after createExecutionOrderingEdges()", "adds ORDER edges", rag.graph);

        rag.removeWrapperNodes();
        mermaid.append("after removeWrapperNodes()", "short-circuit wrappers before concatenate", rag.graph);

        System.out.println("\n\n=========================\n");
        System.out.println("graph = " + rag.graph);

        final List<RagNode> order = rag.getFlattenedExecutionOrder(RagBuilder.DEFAULT_POLICY);
        for (int i = 0; i < order.size() - 1; i++)
            rag.graph.addEdge(order.get(i), order.get(i + 1), FLATTENED_ORDER);
        mermaid.append("flattened execution order", "adds FLATTENED_ORDER edges (just for visualization)", rag.graph);

        mermaid.save("/Users/pietzsch/git/mermaid/irgraph.html");

        System.out.println("\n\n=========================\n");
        System.out.println("graph = " + rag.graph);

//        buildRag(VirtualTableTests.vtMinimal(), "vtMinimal");
//        buildRag(VirtualTableTests.vtLinear(), "vtLinear");
//        buildRag(VirtualTableTests.vtAppend(), "vtAppend");
//        buildRag(VirtualTableTests.vtForkJoin(), "vtForkJoin");
//        buildRag(VirtualTableTests.vtForkJoinLookALike(), "vtForkJoinLookALike");
//        buildRag(VirtualTableTests.vtConcatenate(), "vtConcatenate");
//        buildRag(VirtualTableTests.vtAppendMissing(), "vtAppendMissing");
//        buildRag(VirtualTableTests.vtSimpleMap(), "vtSimpleMap");
//        buildRag(VirtualTableTests.vtSimpleRowFilter(), "vtSimpleRowFilter");
//        buildRag(VirtualTableTests.vtConsecutiveRowFilters(), "vtConsecutiveRowFilters");
//        buildRag(VirtualTableTests.vtMapsAndFilters(), "vtMapsAndFilters");
    }

    public static void buildRag(final VirtualTable table, final String name) {
        var mermaid = new Mermaid();
        var spec =  new SpecGraphBuilder();

        spec.buildSpec(table.getProducingTransform());
        mermaid.append("buildSpec(table)", "SPEC edges", spec.graph);

        var rag = new RagBuilder(spec.graph);

        rag.traceExec();
        mermaid.append("traceExec()", "adds EXEC edges",  rag.graph);

        rag.traceAccesses();
        mermaid.append("traceAccesses()", "adds DATA edges",  rag.graph);

        rag.optimize();
        mermaid.append("optimize()", rag.graph);

        rag.createExecutionOrderingEdges();
        mermaid.append("after createExecutionOrderingEdges()", "adds ORDER edges", rag.graph);

        rag.removeWrapperNodes();
        mermaid.append("after removeWrapperNodes()", "short-circuit wrappers before concatenate", rag.graph);

        final List<RagNode> order = rag.getFlattenedExecutionOrder(RagBuilder.DEFAULT_POLICY);
        for (int i = 0; i < order.size() - 1; i++)
            rag.graph.addEdge(order.get(i), order.get(i + 1), FLATTENED_ORDER);
        mermaid.append("flattened execution order", "adds FLATTENED_ORDER edges (just for visualization)", rag.graph);

        mermaid.save("/Users/pietzsch/git/mermaid/" + name + ".html");
    }
}
