package org.knime.core.table.virtual.graph.rag3;

public class Util {

    static UnsupportedOperationException unhandledNodeType() { // TODO: handle or remove OBSERVER case
        return new UnsupportedOperationException("not handled yet. needs to be implemented or removed");
    }

}
