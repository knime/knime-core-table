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
import java.util.stream.LongStream;

import org.knime.core.table.access.DelegatingReadAccesses;
import org.knime.core.table.access.MissingAccesses;
import org.knime.core.table.access.ReadAccess;

class RandomAccessNodeImpAppend implements RandomAccessNodeImp {

    private final AccessImp[] inputs;

    private final DelegatingReadAccesses.DelegatingReadAccess[] outputs;

    private final RandomAccessNodeImp[] predecessors;

    private final int[][] predecessorOutputIndices;

    private final long[] predecessorSizes;

    /*
     * TODO (TP) revise ?!
     * We split the input row range into sections, such that in each section the
     * same predecessors are active (can provide rows). When moving between
     * sections some predecessor accesses have to be switched to missing or
     * re-linked.
     */

    /**
     * Sequence of start row indices of sections.
     * First element is 0, last element is numRows (of whole table).
     */
    private final long[] sectionStarts;

    /**
     * First row index in current section. (inclusive)
     */
    private long sectionFromRow = -1;

    /**
     * Last row index in current section. (exclusive)
     */
    private long sectionToRow = -1;

    /**
     * Which predecessors are currently linked to accesses.
     * (Accesses for the others are set to missing.)
     */
    private final boolean[] predecessorActive;

    /**
     * TODO (TP) javadoc
     */
    private final RandomAccessNodeImp[] validities;

    /**
     * TODO (TP) javadoc
     */
    private final int[][] validityOutputIndices;

    /**
     * TODO (TP) javadoc
     */
    private final long[] validitySizes;

    /**
     * TODO (TP) javadoc
     */
    private final boolean[] validityLinked;

    /**
     * @param predecessorOutputIndices {@code predecessorOutputIndices[i]} is the list of output indices to switch to
     *            missing when the i-th predecessor is exhausted.
     */
    RandomAccessNodeImpAppend(//
            final AccessImp[] inputs, //
            final RandomAccessNodeImp[] predecessors, //
            final int[][] predecessorOutputIndices, //
            final long[] predecessorSizes, //
            final RandomAccessNodeImp[] validities, //
            final int[][] validityOutputIndices, //
            final long[] validitySizes //
    ) {

        this.inputs = inputs;
        outputs = new DelegatingReadAccesses.DelegatingReadAccess[inputs.length];

        this.predecessors = predecessors;
        this.predecessorOutputIndices = predecessorOutputIndices;
        this.predecessorSizes = predecessorSizes;
        predecessorActive = new boolean[predecessors.length];

        this.validities = validities;
        this.validityOutputIndices = validityOutputIndices;
        this.validitySizes = validitySizes;
        validityLinked = new boolean[validities.length];

        sectionStarts = LongStream.concat( //
                LongStream.concat( //
                        LongStream.of(0), //
                        LongStream.of(predecessorSizes) //
                ), LongStream.of(validitySizes) //
        ).sorted().distinct().toArray();
    }

    @Override
    public ReadAccess getOutput(final int i) {
        return outputs[i];
    }

    /**
     * Point our delegate {@code outputs} to predecessor outputs or to missing
     * access, depending on which section we are in.
     *
     * @param s index of section to link
     */
    private void link(final int s) {

        sectionFromRow = sectionStarts[s];
        sectionToRow = sectionStarts[s + 1];

        for (int p = 0; p < predecessors.length; p++) {
            predecessorActive[p] = predecessorSizes[p] >= sectionToRow;
        }

        for (int v = 0; v < validities.length; v++) {
            final boolean currentlyLinked = validityLinked[v];
            final boolean shouldBeLinked = validitySizes[v] >= sectionToRow;
            if (currentlyLinked && !shouldBeLinked) {
                for (int i : validityOutputIndices[v]) {
                    outputs[i].setMissing();
                }
                validityLinked[v] = false;
            } else if (!currentlyLinked && shouldBeLinked) {
                for (int i : validityOutputIndices[v]) {
                    outputs[i].setDelegateAccess(inputs[i].getReadAccess());
                }
                validityLinked[v] = true;
            }
        }
    }

    @Override
    public void create() {
        for (RandomAccessNodeImp predecessor : predecessors) {
            predecessor.create();
        }
        for (int i = 0; i < inputs.length; i++) {
            final ReadAccess access = inputs[i].getReadAccess();
            outputs[i] = DelegatingReadAccesses.createDelegatingAccess(access.getDataSpec());
        }
    }

    @Override
    public void moveTo(final long row) {
        // NB no bounds checking here, this is done in CapRandomAccessCursor
        if (row < sectionFromRow || row >= sectionToRow) {
            // we are moving to a different section
            int s = Arrays.binarySearch(sectionStarts, row);
            if (s < 0) {
                s = -s - 2;
            }
            link( s );
        }

        for (int i = 0; i < predecessors.length; i++) {
            if (predecessorActive[i]) {
                predecessors[i].moveTo(row);
            }
        }
    }

    @Override
    public void close() throws IOException {
        for (RandomAccessNodeImp predecessor : predecessors) {
            predecessor.close();
        }
    }
}
