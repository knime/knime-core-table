package org.knime.core.table.virtual.graph.rag2;

import org.knime.core.table.virtual.VirtualTable;
import org.knime.core.table.virtual.graph.VirtualTableTests;
import org.knime.core.table.virtual.graph.rag2.SpecGraphBuilder.SpecGraph;

public class RagPlayground2 {

    public static void main(String[] args) {
//        final VirtualTable table = VirtualTableTests.vtMinimal();
//        final VirtualTable table = VirtualTableTests.vtAppendAndSlice();
        final VirtualTable table = VirtualTableTests.vtMapsAndFilters();

        final SpecGraph specGraph = SpecGraphBuilder.buildSpecGraph(table);

        var mermaid = new Mermaid();

        mermaid.append("<pre>" + specGraph + "</pre>");
        mermaid.append("buildSpec(table)", "SPEC edges", specGraph);

        mermaid.save("/Users/pietzsch/git/mermaid/a_graph.html");

        System.out.println("\n\n=========================\n");
        System.out.println("graph = " + specGraph);
    }
}
