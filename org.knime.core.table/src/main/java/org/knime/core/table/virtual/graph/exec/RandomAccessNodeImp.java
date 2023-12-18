package org.knime.core.table.virtual.graph.exec;

interface RandomAccessNodeImp extends NodeImp {

    /**
     * Recursively call {@link #moveTo} on predecessors.
     * Perform any necessary predecessor switching and access buffering. For
     * example, {@link RandomAccessNodeImpConcatenate concatenate} switches to
     * the predecessor containing the specified {@code row}.
     *
     * @param row index of the row to move to
     */
    void moveTo(long row);
}
