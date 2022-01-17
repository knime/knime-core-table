package org.knime.core.table.virtual.graph.exec;

final class AccessImp {
    public final NodeImp node;

    public final int i;

    public AccessImp(NodeImp node, int i) {
        this.node = node;
        this.i = i;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof AccessImp))
            return false;

        final AccessImp other = (AccessImp)o;
        return i == other.i && node.equals(other.node);
    }

    @Override
    public int hashCode() {
        return 31 * node.hashCode() + i;
    }
}
