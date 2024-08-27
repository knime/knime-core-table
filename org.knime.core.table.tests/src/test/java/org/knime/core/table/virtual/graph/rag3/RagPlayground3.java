package org.knime.core.table.virtual.graph.rag3;

import org.knime.core.table.virtual.VirtualTable;
import org.knime.core.table.virtual.graph.VirtualTableTests;
import org.knime.core.table.virtual.graph.cap.CapNode;
import org.knime.core.table.virtual.graph.cap.CursorAssemblyPlan;
import org.knime.core.table.virtual.graph.rag3.debug.DependencyGraph;
import org.knime.core.table.virtual.graph.rag3.debug.Mermaid;

public class RagPlayground3 {

    public static void main(String[] args) {
        final VirtualTable table = VirtualTableTests.vtMinimal();
//        final VirtualTable table = VirtualTableTests.vtAppendAndSlice();
//        final VirtualTable table = VirtualTableTests.vtMapsAndFilters();

        var tableTransformGraph = TableTransformGraph.of(table.getProducingTransform());
        System.out.println("tableTransformGraph = " + tableTransformGraph);

        var dependencyGraph = new DependencyGraph(tableTransformGraph);
        System.out.println("graph = " + dependencyGraph);

        var mermaid = new Mermaid();
        mermaid.append("<pre>" + dependencyGraph + "</pre>");
        mermaid.append("SpecGraph", tableTransformGraph);
        mermaid.save("/Users/pietzsch/git/mermaid/b_graph.html");

        BranchGraph depGraph = new BranchGraph(tableTransformGraph);
        System.out.println("depGraph = " + depGraph);

        CursorAssemblyPlan cap = BuildCap.getCursorAssemblyPlan(depGraph);
        System.out.println();
        System.out.println("cap = ");
        for (CapNode node : cap.nodes()) {
            System.out.println("<" + node.index() + "> " + node);
        }

        var tableTransformGraph2 = tableTransformGraph.copy();
        BranchGraph depGraph2 = new BranchGraph(tableTransformGraph2);
        System.out.println("depGraph2 = " + depGraph2);

        CursorAssemblyPlan cap2 = BuildCap.getCursorAssemblyPlan(depGraph2);
        System.out.println();
        System.out.println("cap2 = ");
        for (CapNode node : cap2.nodes()) {
            System.out.println("<" + node.index() + "> " + node);
        }

    }
}
