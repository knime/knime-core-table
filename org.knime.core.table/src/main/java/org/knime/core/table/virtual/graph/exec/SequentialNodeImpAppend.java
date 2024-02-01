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
