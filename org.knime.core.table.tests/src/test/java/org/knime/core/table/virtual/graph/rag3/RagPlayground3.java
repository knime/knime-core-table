package org.knime.core.table.virtual.graph.rag3;

import org.knime.core.table.virtual.VirtualTable;
import org.knime.core.table.virtual.graph.VirtualTableTests;
import org.knime.core.table.virtual.graph.rag3.SpecGraph.MermaidGraph;
import org.knime.core.table.virtual.graph.rag3.SpecGraph.Terminal;

public class RagPlayground3 {

    public static void main(String[] args) {
//        final VirtualTable table = VirtualTableTests.vtMinimal();
//        final VirtualTable table = VirtualTableTests.vtAppendAndSlice();
        final VirtualTable table = VirtualTableTests.vtMapsAndFilters();

        Terminal terminal = SpecGraph.buildSpecGraph(table.getProducingTransform());
        System.out.println("terminal = " + terminal);

        MermaidGraph graph = new MermaidGraph(terminal);
        System.out.println("graph = " + graph);

        var mermaid = new Mermaid();
        mermaid.append("<pre>" + graph + "</pre>");
        mermaid.append("SpecGraph", graph);
        mermaid.save("/Users/pietzsch/git/mermaid/b_graph.html");


    }
}
