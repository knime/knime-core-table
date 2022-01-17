package org.knime.core.table.virtual.graph.exec;

import java.util.Arrays;

import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.row.ReadAccessRow;

class CapReadAccessRow implements ReadAccessRow {

    private final ReadAccess[] accesses;

    CapReadAccessRow(final NodeImpConsumer node) {
        accesses = new ReadAccess[node.numOutputs()];
        Arrays.setAll(accesses, node::getOutput);
    }

    @Override
    public int size() {
        return accesses.length;
    }

    @Override
    public <A extends ReadAccess> A getAccess(int index) {
        return (A)accesses[index];
    }
}
