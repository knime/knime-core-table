package org.knime.core.table.virtual.graph.exec;

import java.io.IOException;

import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.virtual.spec.ObserverTransformSpec;

class SequentialNodeImpObserver implements SequentialNodeImp {

    private final AccessImp[] inputs;

    private final ReadAccess[] observerInputs;

    private final ObserverTransformSpec.ObserverFactory observerFactory;

    private Runnable observer;

    private final SequentialNodeImp predecessor;

    SequentialNodeImpObserver(//
        final AccessImp[] inputs, //
        final SequentialNodeImp predecessor, //
        final ObserverTransformSpec.ObserverFactory observerFactory) {

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
        observer = observerFactory.createObserver(observerInputs);
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
