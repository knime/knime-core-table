package org.knime.core.table.virtual.graph.rag;

import static org.knime.core.table.virtual.graph.rag.RagEdgeType.FLATTENED_ORDER;

import java.util.List;

import org.knime.core.table.virtual.VirtualTable;
import org.knime.core.table.virtual.graph.VirtualTableExamples;
import org.knime.core.table.virtual.graph.util.Mermaid;

public class RagPlayground {

    public static void main(final String[] args) {
//        final VirtualTable table = VirtualTableExamples.vtMinimal();
//        final VirtualTable table = VirtualTableExamples.vtLinear();
//        final VirtualTable table = VirtualTableExamples.vtAppend();
//        final VirtualTable table = VirtualTableExamples.vtAppendAndSlice();
//        final VirtualTable table = VirtualTableExamples.vtForkJoin();
//        final VirtualTable table = VirtualTableExamples.vtForkJoinLookALike();
//        final VirtualTable table = VirtualTableExamples.vtConcatenate();
//        final VirtualTable table = VirtualTableExamples.vtConcatenateAndSlice();
//        final VirtualTable table = VirtualTableExamples.vtConcatenateAndSliceSingleTable();
//        final VirtualTable table = VirtualTableExamples.vtConcatenateAndSliceFullSingleTable();
//        final VirtualTable table = VirtualTableExamples.vtConcatenateAndSliceFullTable();
//        final VirtualTable table = VirtualTableExamples.vtAppendMissing();
//        final VirtualTable table = VirtualTableExamples.vtSimpleMap();
//        final VirtualTable table = VirtualTableExamples.vtSimpleRowFilter();
//        final VirtualTable table = VirtualTableExamples.vtConsecutiveRowFilters();
//        final VirtualTable table = VirtualTableExamples.vtMapsAndFilters();
//        final VirtualTable table = VirtualTableExamples.vtFiltersMapAndConcatenate();
        final VirtualTable table = VirtualTableExamples.vtMaterializeMinimal();

        var mermaid = new Mermaid();
        var rag = new RagBuilder();

        rag.buildSpec(table.getProducingTransform());
        mermaid.append("buildSpec(table)", "SPEC edges", rag.graph);

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

//        buildRag(VirtualTableExamples.vtMinimal(), "vtMinimal");
//        buildRag(VirtualTableExamples.vtLinear(), "vtLinear");
//        buildRag(VirtualTableExamples.vtAppend(), "vtAppend");
//        buildRag(VirtualTableExamples.vtForkJoin(), "vtForkJoin");
//        buildRag(VirtualTableExamples.vtForkJoinLookALike(), "vtForkJoinLookALike");
//        buildRag(VirtualTableExamples.vtConcatenate(), "vtConcatenate");
//        buildRag(VirtualTableExamples.vtAppendMissing(), "vtAppendMissing");
//        buildRag(VirtualTableExamples.vtSimpleMap(), "vtSimpleMap");
//        buildRag(VirtualTableExamples.vtSimpleRowFilter(), "vtSimpleRowFilter");
//        buildRag(VirtualTableExamples.vtConsecutiveRowFilters(), "vtConsecutiveRowFilters");
//        buildRag(VirtualTableExamples.vtMapsAndFilters(), "vtMapsAndFilters");
    }

    public static void buildRag(final VirtualTable table, final String name) {
        var mermaid = new Mermaid();
        var rag = new RagBuilder();

        rag.buildSpec(table.getProducingTransform());
        mermaid.append("buildSpec(table)", "SPEC edges", rag.graph);

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
