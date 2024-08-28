package org.knime.core.table.virtual.graph.rag3;

import org.knime.core.table.virtual.VirtualTable;
import org.knime.core.table.virtual.graph.VirtualTableTests;
import org.knime.core.table.virtual.graph.cap.CapNode;
import org.knime.core.table.virtual.graph.cap.CursorAssemblyPlan;
import org.knime.core.table.virtual.graph.rag3.debug.DependencyGraph;
import org.knime.core.table.virtual.graph.rag3.debug.Mermaid;

public class RagPlayground3 {

    public static void main(String[] args) {
//        final VirtualTable table = VirtualTableTests.vtMinimal();
//        final VirtualTable table = VirtualTableTests.vtAppend();
//        final VirtualTable table = VirtualTableTests.vtAppendAndSlice();
//        final VirtualTable table = VirtualTableTests.vtMapsAndFilters();
        final VirtualTable table = VirtualTableTests.vtLinear().slice(2,4).slice(0,1);

        var tableTransformGraph = new TableTransformGraph(table.getProducingTransform());
        System.out.println("tableTransformGraph = " + tableTransformGraph);

        TableTransformUtil.pruneAccesses(tableTransformGraph);

        var dependencyGraph = new DependencyGraph(tableTransformGraph);
        System.out.println("graph = " + dependencyGraph);

        var mermaid = new Mermaid();
        mermaid.append("<pre>" + dependencyGraph + "</pre>");
        mermaid.append("SpecGraph", tableTransformGraph);

        while(TableTransformUtil.mergeSlices(tableTransformGraph)) {
        }
        mermaid.append("SpecGraph", tableTransformGraph);

        BranchGraph depGraph = new BranchGraph(tableTransformGraph);
        System.out.println("depGraph = " + depGraph);
        mermaid.append("BranchGraph", depGraph);
        mermaid.save("/Users/pietzsch/git/mermaid/b_graph.html");

        CursorAssemblyPlan cap = BuildCap.createCursorAssemblyPlan(depGraph);
        System.out.println();
        System.out.println("cap = ");
        for (CapNode node : cap.nodes()) {
            System.out.println("<" + node.index() + "> " + node);
        }
    }
}
