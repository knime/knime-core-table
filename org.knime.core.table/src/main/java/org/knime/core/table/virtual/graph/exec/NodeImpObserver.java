package org.knime.core.table.virtual.graph.exec;

import java.io.IOException;

import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.virtual.spec.ProgressListenerTransformSpec;

class NodeImpObserver implements NodeImp {

    private final AccessImp[] inputs;

    private final ReadAccess[] observerInputs;

    private final ProgressListenerTransformSpec.ProgressListenerFactory observerFactory;

    private Runnable observer;

    private final NodeImp predecessor;

    NodeImpObserver(//
        final AccessImp[] inputs, //
        final NodeImp predecessor, //
        final ProgressListenerTransformSpec.ProgressListenerFactory observerFactory) {

        this.inputs = inputs;
        this.predecessor = predecessor;
        observerInputs = new ReadAccess[inputs.length];
        this.observerFactory = observerFactory;
    }

    @Override
    public ReadAccess getOutput(final int i) {
        // Observer doesn't have outputs
        throw new UnsupportedOperationException();
    }

    private void link() {
        for (int i = 0; i < inputs.length; i++) {
            observerInputs[i] = inputs[i].getReadAccess();
        }
        observer = observerFactory.createProgressListener(observerInputs);
    }

    @Override
    public void create() {
        predecessor.create();
        link();
    }

    @Override
    public boolean forward() {
        if (predecessor.forward()) {
            observer.run();
            return true;
        } else {
            return false;
        }
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
