package org.knime.core.table.virtual.graph.exec;

import java.io.IOException;

import org.knime.core.table.access.ReadAccess;

interface NodeImp {
    /**
     *
     * @param i slot index
     * @return
     */
    ReadAccess getOutput(int i);

    /**
     * Recursively call {@link #create} on all predecessors. Then do any setup
     * that this {@code NodeImp} itself requires. After {@link #create}, the
     * output {@code ReadAccess}es of this {@code NodeImp} must be available via
     * {@link #getOutput}.
     */
    void create();

    /**
     * Recursively call {@link #forward} on predecessors, possibly multiple
     * times. For example, {@link NodeImpRowFilter row filter} repeatedly calls
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

    /**
     * Recursively call {@link #close} on all predecessors. Then do any clean-up
     * this {@code NodeImp} itself requires.
     *
     * @throws IOException
     */
    void close() throws IOException;
}
