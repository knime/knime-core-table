/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 */
package org.knime.core.table.virtual.graph.exec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.knime.core.table.access.DelegatingReadAccesses;
import org.knime.core.table.access.ReadAccess;

class SequentialNodeImpConcatenate implements SequentialNodeImp {
    private final AccessImp[][] inputss;

    private final DelegatingReadAccesses.DelegatingReadAccess[] outputs;

    private final SequentialNodeImp[] predecessors;

    private final List<IOException> exceptionsWhileClosing;

    private int predecessorIndex;

    private SequentialNodeImp predecessor;

    private SequentialNodeImp linkedPredecessor;

    SequentialNodeImpConcatenate(final AccessImp[][] inputs, final SequentialNodeImp[] predecessors) {
        if (inputs.length != predecessors.length) {
            throw new IllegalArgumentException();
        }
        final int numOutputs = inputs[0].length;
        for (int i = 1; i < inputs.length; i++) {
            if (inputs[i].length != numOutputs) {
                throw new IllegalArgumentException();
            }
        }

        this.inputss = inputs;
        this.predecessors = predecessors;
        exceptionsWhileClosing = new ArrayList<>();
        outputs = new DelegatingReadAccesses.DelegatingReadAccess[numOutputs];
    }

    /**
     * Close {@code node}, catch and record {@code IOException} for re-throwing later.
     */
    private void tryClose(final SequentialNodeImp node) {
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
    public ReadAccess getOutput(final int i) {
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
        if (!exceptionsWhileClosing.isEmpty()) {
            // TODO use IOExceptionList once org.apache.commons.io >= 2.7.0 is available in the nightlies
            throw exceptionsWhileClosing.get(0);
        }
    }
}
