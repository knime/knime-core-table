package org.knime.core.table.virtual.graph.rag3;

import org.knime.core.table.virtual.VirtualTable;
import org.knime.core.table.virtual.graph.VirtualTableTests;

public class RagPlayground3 {

    public static void main(String[] args) {
        final VirtualTable table = VirtualTableTests.vtMinimal();
//        final VirtualTable table = VirtualTableTests.vtAppendAndSlice();
//        final VirtualTable table = VirtualTableTests.vtMapsAndFilters();

        SpecGraph.Terminal specGraph = SpecGraph.buildSpecGraph(table.getProducingTransform());

        System.out.println("specGraph = " + specGraph);

    }
}
