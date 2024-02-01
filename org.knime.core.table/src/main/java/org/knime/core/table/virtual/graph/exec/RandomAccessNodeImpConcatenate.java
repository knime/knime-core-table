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
import java.util.Arrays;

import org.knime.core.table.access.DelegatingReadAccesses;
import org.knime.core.table.access.ReadAccess;

class RandomAccessNodeImpConcatenate implements RandomAccessNodeImp {

    private final AccessImp[][] inputss;

    private final DelegatingReadAccesses.DelegatingReadAccess[] outputs;

    private final RandomAccessNodeImp[] predecessors;

    private final long[] predecessorStarts;

    /**
     * The currently {@link #link linked} predecessor.
     */
    private RandomAccessNodeImp predecessor;

    /**
     * First row index in currently linked predecessor. (inclusive)
     */
    private long predecessorFromRow = -1;

    /**
     * Last row index in currently linked predecessor + 1. (exclusive)
     */
    private long predecessorToRow = -1;

    RandomAccessNodeImpConcatenate(//
        final AccessImp[][] inputs, //
        final RandomAccessNodeImp[] predecessors, //
        final long[] predecessorSizes) {

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
        predecessorStarts = computeStartRowIndices(predecessorSizes);
        outputs = new DelegatingReadAccesses.DelegatingReadAccess[numOutputs];
    }

    /**
     * Compute the index of the first row in each predecessor.
     * <p>
     * For example, {@code sizes={5, 3, 3}} means that there are 3 predecessors to
     * concatenate, with 5, 3, and 3 rows respectively.
     * The returned array would be {@code starts={0, 5, 8, 11}}, meaning that
     * row index 0 is the first to fall into predecessor 0,
     * row index 5 is the first to fall into predecessor 1,
     * row index 8 is the first to fall into predecessor 2,
     * and row index 11 is the first out of bounds.
     */
    private static long[] computeStartRowIndices(final long[] sizes) {
        final long[] starts = new long[sizes.length + 1];
        for (int i = 0; i < sizes.length; ++i) {
            starts[i + 1] = starts[i] + sizes[i];
        }
        return starts;
    }

    /**
     * Point our delegate {@code outputs} to predecessor {@code p}.
     * Also set {@code linkedPredecessor} to that predecessor.
     * {@code outputs} delegates are initialized on first call.
     *
     * @param p index of predecessor to link
     */
    private void link(final int p) {
        AccessImp[] inputs = inputss[p];
        for (int i = 0; i < inputs.length; i++) {
            final ReadAccess access = inputs[i].getReadAccess();
            outputs[i].setDelegateAccess(access);
        }
        predecessor = predecessors[p];
        predecessorFromRow = predecessorStarts[p];
        predecessorToRow = predecessorStarts[p + 1];
    }

    @Override
    public ReadAccess getOutput(final int i) {
        return outputs[i];
    }

    @Override
    public void create() {
        for (RandomAccessNodeImp predecessor : predecessors) {
            predecessor.create();
        }
        for (int i = 0; i < outputs.length; i++) {
            final ReadAccess access = inputss[0][i].getReadAccess();
            outputs[i] = DelegatingReadAccesses.createDelegatingAccess(access.getDataSpec());
        }
    }

    @Override
    public void moveTo(final long row) {
        // NB no bounds checking here, this is done in CapRandomAccessCursor
        if (row < predecessorFromRow || row >= predecessorToRow) {
            // we are moving to a different predecessor
            int p = Arrays.binarySearch(predecessorStarts, row);
            if (p < 0) {
                p = -p - 2;
            }
            link( p );
        }

        predecessor.moveTo(row - predecessorFromRow);
    }

    @Override
    public void close() throws IOException {
        for (RandomAccessNodeImp predecessor : predecessors) {
            predecessor.close();
        }
    }
}
