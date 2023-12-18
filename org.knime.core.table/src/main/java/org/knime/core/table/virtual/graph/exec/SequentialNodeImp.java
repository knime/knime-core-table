package org.knime.core.table.virtual.graph.exec;

interface SequentialNodeImp extends NodeImp {

    /**
     * Recursively call {@link #forward} on predecessors, possibly multiple
     * times. For example, {@link SequentialNodeImpRowFilter row filter} repeatedly calls
     * {@code predecessor.forward()} until the filter predicate accepts the
     * current values (current row).
     *
     * @return {@code true} if forwarding was successful, {@code false}
     *         otherwise (i.e. at the end)
     */
    boolean forward();

    /*
     * Recursively call {@link #canForward} on predecessors.
     * <p>
     * Some {@code NodeImp}s simply throw an exception because they cannot
     * figure out whether it is possible to forward without actually doing so.
     * {@code #canForward} will be used in the created cursor only if the comp
     * graph figured out that it is possible.
     *
     * @return {@code true} if more elements are available, that is, the next
     *         {@link #forward} would return {@code true}.
     */
    boolean canForward();

}
