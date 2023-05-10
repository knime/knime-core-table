package org.knime.core.table.virtual.graph.exec;

import org.knime.core.table.virtual.graph.exec.AbstractNodeImp;

interface RandomAccessNodeImp extends AbstractNodeImp {

    /**
     * TODO javadoc
     * @param row
     */
    void moveTo(long row);
}
