package org.knime.core.table.virtual.graph.exec;

import java.io.IOException;

import org.knime.core.table.access.ReadAccess;

class SequentialNodeImpConsumer implements SequentialNodeImp {

    private final AccessImp[] inputs;

    private final SequentialNodeImp predecessor;

    private final ReadAccess[] outputs;

    SequentialNodeImpConsumer(final AccessImp[] inputs, final SequentialNodeImp predecessor) {
        this.inputs = inputs;
        this.predecessor = predecessor;
        outputs = new ReadAccess[inputs.length];
    }

    @Override
    public ReadAccess getOutput(final int i) {
        return outputs[i];
    }

    public int numOutputs() {
        return outputs.length;
    }

    private void link() {
        for (int i = 0; i < inputs.length; i++) {
            outputs[i] = inputs[i].getReadAccess();
        }
    }

    @Override
    public void create() {
        predecessor.create();
        link();
    }

    @Override
    public boolean forward() {
        return predecessor.forward();
    }

    @Override
    public boolean canForward() {
        return predecessor.canForward();
    }

    @Override
    public void close() throws IOException {
        predecessor.close();
    }
}
