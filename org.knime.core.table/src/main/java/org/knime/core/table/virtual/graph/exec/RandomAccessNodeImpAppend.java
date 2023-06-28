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
    private final boolean[] linked;

    /**
     * @param predecessorOutputIndices {@code predecessorOutputIndices[i]} is the list of output indices to switch to
     *            missing when the i-th predecessor is exhausted.
     */
    RandomAccessNodeImpAppend(//
        final AccessImp[] inputs, //
        final RandomAccessNodeImp[] predecessors, //
        final int[][] predecessorOutputIndices, //
        final long[] predecessorSizes) {

        this.inputs = inputs;
        outputs = new DelegatingReadAccesses.DelegatingReadAccess[inputs.length];

        this.predecessors = predecessors;
        this.predecessorOutputIndices = predecessorOutputIndices;
        this.predecessorSizes = predecessorSizes;
        sectionStarts = LongStream.concat( //
                LongStream.of(0), LongStream.of(predecessorSizes) //
        ).sorted().distinct().toArray();
        linked = new boolean[predecessors.length];
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
            final boolean currentlyActive = linked[p];
            final boolean shouldBeActive = predecessorSizes[p] >= sectionToRow;
            if ( currentlyActive && !shouldBeActive )
            {
                for (int i : predecessorOutputIndices[p]) {
                    final DelegatingReadAccesses.DelegatingReadAccess output = outputs[i];
                    output.setDelegateAccess(MissingAccesses.getMissingAccess(output.getDataSpec()));
                }
                linked[ p ] = false;
            }
            else if ( !currentlyActive && shouldBeActive )
            {
                for (int i : predecessorOutputIndices[p]) {
                    final DelegatingReadAccesses.DelegatingReadAccess output = outputs[i];
                    output.setDelegateAccess(inputs[i].getReadAccess());
                }
                linked[ p ] = true;
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
            if (linked[i]) {
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
