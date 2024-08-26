package org.knime.core.table.virtual.graph.rag3;

import org.knime.core.table.virtual.VirtualTable;
import org.knime.core.table.virtual.graph.VirtualTableTests;
import org.knime.core.table.virtual.graph.cap.CapNode;
import org.knime.core.table.virtual.graph.cap.CursorAssemblyPlan;
import org.knime.core.table.virtual.graph.rag3.SpecGraph.BuildCap;
import org.knime.core.table.virtual.graph.rag3.SpecGraph.DependencyGraph;
import org.knime.core.table.virtual.graph.rag3.SpecGraph.MermaidGraph;
import org.knime.core.table.virtual.graph.rag3.SpecGraph.Terminal;

public class RagPlayground3 {

    public static void main(String[] args) {
        final VirtualTable table = VirtualTableTests.vtMinimal();
//        final VirtualTable table = VirtualTableTests.vtAppendAndSlice();
//        final VirtualTable table = VirtualTableTests.vtMapsAndFilters();

        Terminal terminal = SpecGraph.buildSpecGraph(table.getProducingTransform());
        System.out.println("terminal = " + terminal);

        MermaidGraph graph = new MermaidGraph(terminal);
        System.out.println("graph = " + graph);

        var mermaid = new Mermaid();
        mermaid.append("<pre>" + graph + "</pre>");
        mermaid.append("SpecGraph", graph);
        mermaid.save("/Users/pietzsch/git/mermaid/b_graph.html");

        DependencyGraph depGraph = new DependencyGraph(terminal);
        System.out.println("depGraph = " + depGraph);

        CursorAssemblyPlan cap = BuildCap.getCursorAssemblyPlan(depGraph);
        System.out.println();
        System.out.println("cap = ");
        for (CapNode node : cap.nodes()) {
            System.out.println("<" + node.index() + "> " + node);
        }



        final Terminal terminal2 = SpecGraph.copy(terminal);
        DependencyGraph depGraph2 = new DependencyGraph(terminal2);
        System.out.println("depGraph2 = " + depGraph2);

        CursorAssemblyPlan cap2 = BuildCap.getCursorAssemblyPlan(depGraph2);
        System.out.println();
        System.out.println("cap2 = ");
        for (CapNode node : cap2.nodes()) {
            System.out.println("<" + node.index() + "> " + node);
        }

    }
}
