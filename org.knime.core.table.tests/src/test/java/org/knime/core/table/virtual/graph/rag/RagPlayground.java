package org.knime.core.table.virtual.graph.rag;

import static org.knime.core.table.virtual.graph.rag.RagEdgeType.FLATTENED_ORDER;

import java.util.List;

import org.knime.core.table.virtual.VirtualTable;
import org.knime.core.table.virtual.graph.VirtualTableTests;
import org.knime.core.table.virtual.graph.util.Mermaid;

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
        mermaid.append("buildSpec(table)", "SPEC edges", spec.graph);

        var rag = new RagBuilder(spec.graph);

        rag.traceAccesses();
        rag.traceExec();
        mermaid.append("traceAccesses(); traceExec()", "adds DATA, EXEC edges",  rag.graph);

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

        rag.traceAccesses();
        mermaid.append("traceAccesses()", "adds DATA edges",  rag.graph);

        rag.traceExec();
        mermaid.append("traceAccesses(); traceExec()", "adds DATA, EXEC edges",  rag.graph);

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
