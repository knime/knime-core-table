package org.knime.core.table.virtual.graph.exec;

import java.io.IOException;

import org.knime.core.table.access.ReadAccess;

interface AbstractNodeImp {

    /**
     * Get the {@code ReadAccess} at the {@code i}-th output slot of this {@code
     * NodeImp}.
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
     * Recursively call {@link #close} on all predecessors. Then do any clean-up
     * this {@code NodeImp} itself requires.
     *
     * @throws IOException
     */
    void close() throws IOException;
}
