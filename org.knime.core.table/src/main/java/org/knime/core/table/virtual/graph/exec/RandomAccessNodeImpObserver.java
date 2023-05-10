package org.knime.core.table.virtual.graph.exec;

import java.io.IOException;

import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.virtual.spec.ObserverTransformSpec;

class RandomAccessNodeImpObserver implements RandomAccessNodeImp {

    private final AccessImp[] inputs;

    private final ReadAccess[] observerInputs;

    private final ObserverTransformSpec.ObserverFactory observerFactory;

    private Runnable observer;

    private final RandomAccessNodeImp predecessor;

    RandomAccessNodeImpObserver(final AccessImp[] inputs, final RandomAccessNodeImp predecessor, final ObserverTransformSpec.ObserverFactory observerFactory) {
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
    public void moveTo(final long row) {
        // NB no bounds checking here, this is done in CapRandomAccessCursor
        predecessor.moveTo(row);
        observer.run();
    }

    @Override
    public void close() throws IOException {
        predecessor.close();
    }
}
