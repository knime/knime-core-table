package org.knime.core.table.virtual.graph.rag2;

import org.knime.core.table.virtual.VirtualTable;
import org.knime.core.table.virtual.graph.VirtualTableTests;

public class RagPlayground2 {

    public static void main(String[] args) {
        final VirtualTable table = VirtualTableTests.vtMinimal();
        final SpecGraphBuilder.SpecGraph specGraph = SpecGraphBuilder.buildSpecGraph(table);
        System.out.println("specGraph = " + specGraph);
    }
}
