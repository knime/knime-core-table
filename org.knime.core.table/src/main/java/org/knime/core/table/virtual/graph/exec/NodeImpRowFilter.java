package org.knime.core.table.virtual.graph.exec;

import java.io.IOException;

import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.virtual.spec.RowFilterTransformSpec;

class NodeImpRowFilter implements NodeImp {
    private final AccessImp[] inputs;

    private final ReadAccess[] filterInputs;

    private final NodeImp predecessor;

    private final RowFilterTransformSpec.RowFilter filter;

    public NodeImpRowFilter(final AccessImp[] inputs, final NodeImp predecessor,
            final RowFilterTransformSpec.RowFilter filter) {
        this.inputs = inputs;
        this.predecessor = predecessor;
        this.filter = filter;
        filterInputs = new ReadAccess[inputs.length];
    }

    @Override
    public ReadAccess getOutput(int i) {
        // RowFilter doesn't have outputs
        throw new UnsupportedOperationException();
    }

    private void link() {
        for (int i = 0; i < inputs.length; i++) {
            AccessImp input = inputs[i];
            filterInputs[i] = input.node.getOutput(input.i);
        }
    }

    @Override
    public void create() {
        predecessor.create();
        link();
    }

    @Override
    public boolean forward() {
        while (predecessor.forward()) {
            if (filter.test(filterInputs))
                return true;
        }
        return false;
    }

    @Override
    public boolean canForward() {
        // RowFilter doesn't have lookahead
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        predecessor.close();
    }
}
