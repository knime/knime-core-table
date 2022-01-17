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

    private void link(final int predecessorIndex) {
        if (predecessorIndex < predecessors.length) {
            AccessImp[] inputs = inputss[predecessorIndex];
            for (int i = 0; i < inputs.length; i++) {
                AccessImp input = inputs[i];
                final ReadAccess access = input.node.getOutput(input.i);
                if (predecessorIndex == 0) {
                    final DelegatingReadAccesses.DelegatingReadAccess delegated =
                            DelegatingReadAccesses.createDelegatingAccess(access.getDataSpec());
                    outputs[i] = delegated;
                }
                outputs[i].setDelegateAccess(access);
            }
        } else {
            for (DelegatingReadAccesses.DelegatingReadAccess output : outputs) {
                output.setDelegateAccess(MissingAccesses.getMissingAccess(output.getDataSpec()));
            }
        }
    }

    @Override
    public ReadAccess getOutput(int i) {
        return outputs[i];
    }

    private void nextPredecessor() {
        closePredecessor();
        ++predecessorIndex;
        if (predecessorIndex < predecessors.length) {
            predecessors[predecessorIndex].create();
            predecessor = predecessors[predecessorIndex];
        } else {
            predecessor = null;
        }
        link(predecessorIndex);
    }

    private void closePredecessor() {
        if (predecessor != null) {
            try {
                predecessor.close();
            } catch (IOException e) {
                exceptionsWhileClosing.add(e);
            }
        }
    }

    @Override
    public void create() {
        predecessorIndex = -1;
        nextPredecessor();
    }

    @Override
    public boolean forward() {
        while (predecessor != null) {
            if (predecessor.forward()) {
                return true;
            } else {
                nextPredecessor();
            }
        }
        return false;
    }

    @Override
    public void close() throws IOException {
        closePredecessor();
        if (!exceptionsWhileClosing.isEmpty())
            // TODO use IOExceptionList once org.apache.commons.io >= 2.7.0 is available in the nightlies
            throw exceptionsWhileClosing.get(0);
    }
}
