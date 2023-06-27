package org.knime.core.table.virtual.graph.exec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.knime.core.table.access.DelegatingReadAccesses;
import org.knime.core.table.access.MissingAccesses;
import org.knime.core.table.access.ReadAccess;

class NodeImpConcatenate implements NodeImp {
    private final AccessImp[][] inputss;

    private final DelegatingReadAccesses.DelegatingReadAccess[] outputs;

    private final NodeImp[] predecessors;

    private final List<IOException> exceptionsWhileClosing;

    private int predecessorIndex;

    private NodeImp predecessor;

    private NodeImp linkedPredecessor;

    public NodeImpConcatenate(AccessImp[][] inputs, NodeImp[] predecessors) {
        if (inputs.length != predecessors.length)
            throw new IllegalArgumentException();
        final int numOutputs = inputs[0].length;
        for (int i = 1; i < inputs.length; i++) {
            if (inputs[i].length != numOutputs)
                throw new IllegalArgumentException();
        }

        this.inputss = inputs;
        this.predecessors = predecessors;
        exceptionsWhileClosing = new ArrayList<>();
        outputs = new DelegatingReadAccesses.DelegatingReadAccess[numOutputs];
    }

    /**
     * Close {@code node}, catch and record {@code IOException} for re-throwing later.
     */
    private void tryClose(final NodeImp node) {
        try {
            node.close();
        } catch (IOException e) {
            exceptionsWhileClosing.add(e);
        }
    }

    /**
     * Close {@code linkedPredecessor} and set to {@code null}.
     */
    private void closeLinkedPredecessor() {
        if (linkedPredecessor != null) {
            tryClose(linkedPredecessor);
            linkedPredecessor = null;
        }
    }

    /**
     * Point our delegate {@code outputs} to predecessor {@code predecessorIndex}.
     * Also set {@code linkedPredecessor} to that predecessor.
     * {@code outputs} delegates are initialized on first call.
     */
    private void link() {
        if (predecessorIndex < predecessors.length) {
            AccessImp[] inputs = inputss[predecessorIndex];
            for (int i = 0; i < inputs.length; i++) {
                final ReadAccess access = inputs[i].getReadAccess();
                if (outputs[i] == null) {
                    outputs[i] = DelegatingReadAccesses.createDelegatingAccess(access.getDataSpec());
                }
                outputs[i].setDelegateAccess(access);
            }
            linkedPredecessor = predecessors[predecessorIndex];
        }
    }

    /**
     * Move to the next predecessor.
     * This sets {@code predecessor} and {@code predecessorIndex}, but does not link the predecessor yet.
     */
    private void nextPredecessor() {
        ++predecessorIndex;
        if (predecessorIndex < predecessors.length) {
            predecessors[predecessorIndex].create();
            predecessor = predecessors[predecessorIndex];
        } else {
            predecessor = null;
        }
    }

    @Override
    public ReadAccess getOutput(int i) {
        return outputs[i];
    }

    @Override
    public void create() {
        predecessorIndex = -1;
        nextPredecessor();
        link();
    }

    @Override
    public boolean forward() {
        while (predecessor != null) {
            // NB: canForward() might have moved predecessor ahead of the currently
            // linkedPredecessor.
            if (linkedPredecessor != predecessor) {
                closeLinkedPredecessor();
                link();
            } else {
                if (predecessor.forward()) {
                    return true;
                } else {
                    nextPredecessor();
                }
            }
        }
        return false;
    }

    @Override
    public boolean canForward() {
        while (predecessor != null) {
            if (predecessor.canForward()) {
                return true;
            } else {
                // We are at the last row of the current predecessor. Go to the next non-empty
                // predecessor, but don't link it yet.
                if ( predecessor != linkedPredecessor ) {
                    // This can happen if one of the concatenated tables is empty... We are still
                    // linked to the last row of linkedPredecessor. The current predecessor is empty
                    // (and was never linked), so we close it here before skipping to the next one.
                    tryClose(predecessor);
                }
                nextPredecessor();
            }
        }
        return false;
    }

    @Override
    public void close() throws IOException {
        closeLinkedPredecessor();
        if (!exceptionsWhileClosing.isEmpty())
            // TODO use IOExceptionList once org.apache.commons.io >= 2.7.0 is available in the nightlies
            throw exceptionsWhileClosing.get(0);
    }
}
