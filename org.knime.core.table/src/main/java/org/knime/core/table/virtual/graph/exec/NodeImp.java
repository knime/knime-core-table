package org.knime.core.table.virtual.graph.exec;

import java.io.IOException;

import org.knime.core.table.access.ReadAccess;

interface NodeImp {
    ReadAccess getOutput(int i);

    void create();

    boolean forward();

    boolean canForward();

    void close() throws IOException;
}
