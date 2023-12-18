package org.knime.core.table.virtual.graph.exec;

import java.io.IOException;

import org.knime.core.table.access.DelegatingReadAccesses;
import org.knime.core.table.access.MissingAccesses;
import org.knime.core.table.access.ReadAccess;

class SequentialNodeImpAppend implements SequentialNodeImp {
    private final AccessImp[] inputs;

    private final DelegatingReadAccesses.DelegatingReadAccess[] outputs;

    private final SequentialNodeImp[] predecessors;

    private final int[][] predecessorOutputIndices;

    private final boolean[] exhausted;

    /**
     * @param predecessorOutputIndices {@code predecessorOutputIndices[i]} is the list
     *                                 of output indices to switch to missing when the i-th predecessor is exhausted.
     */
    SequentialNodeImpAppend(final AccessImp[] inputs, final SequentialNodeImp[] predecessors, final int[][] predecessorOutputIndices) {
        this.inputs = inputs;
        outputs = new DelegatingReadAccesses.DelegatingReadAccess[inputs.length];

        this.predecessors = predecessors;
        this.predecessorOutputIndices = predecessorOutputIndices;
        exhausted = new boolean[predecessors.length];
    }

    @Override
    public ReadAccess getOutput(final int i) {
        return outputs[i];
    }

    private void link() {
        for (int i = 0; i < inputs.length; i++) {
            final ReadAccess access = inputs[i].getReadAccess();
            final DelegatingReadAccesses.DelegatingReadAccess delegated =
                    DelegatingReadAccesses.createDelegatingAccess(access.getDataSpec());
            delegated.setDelegateAccess(access);
            outputs[i] = delegated;
        }
    }

    @Override
    public void create() {
        for (SequentialNodeImp predecessor : predecessors) {
            predecessor.create();
        }
        link();
    }

    @Override
    public boolean forward() {
        boolean anyForwarded = false;
        for (int i = 0; i < predecessors.length; i++) {
            final SequentialNodeImp predecessor = predecessors[i];
            if (predecessor.forward()) {
                anyForwarded = true;
            } else {
                if (!exhausted[i]) {
                    exhausted[i] = true;
                    for (int o : predecessorOutputIndices[i]) {
                        final DelegatingReadAccesses.DelegatingReadAccess output = outputs[o];
                        output.setDelegateAccess(MissingAccesses.getMissingAccess(output.getDataSpec()));
                    }
                }
            }
        }
        return anyForwarded;
    }

    @Override
    public boolean canForward() {
        for (var predecessor : predecessors) {
            if (predecessor.canForward()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void close() throws IOException {
        for (SequentialNodeImp predecessor : predecessors) {
            predecessor.close();
        }
    }
}
