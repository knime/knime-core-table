package org.knime.core.table.virtual.graph.exec;

import java.io.IOException;

import org.knime.core.table.access.ReadAccess;

class RandomAccessNodeImpConsumer implements RandomAccessNodeImp {

    private final AccessImp[] inputs;

    private final RandomAccessNodeImp predecessor;

    private final ReadAccess[] outputs;

    RandomAccessNodeImpConsumer(final AccessImp[] inputs, final RandomAccessNodeImp predecessor) {
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
    public void moveTo(final long row) {
        predecessor.moveTo(row);
    }

    @Override
    public void close() throws IOException {
        predecessor.close();
    }
}
