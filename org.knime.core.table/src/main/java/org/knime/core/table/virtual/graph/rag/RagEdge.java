package org.knime.core.table.virtual.graph.rag;

public final class RagEdge implements Typed<RagEdgeType> {

    private final RagNode source;

    private final RagNode target;

    private final RagEdgeType type;

    RagEdge(final RagNode source, final RagNode target, final RagEdgeType type) {
        this.source = source;
        this.target = target;
        this.type = type;
    }

    public RagNode getSource() {
        return source;
    }

    public RagNode getTarget() {
        return target;
    }

    @Override
    public RagEdgeType type() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof RagEdge))
            return false;
        final RagEdge edge = (RagEdge)o;
        return source.equals(edge.source) && (target.equals(edge.target)) && (type == edge.type);
    }

    @Override
    public int hashCode() {
        int result = source != null ? source.hashCode() : 0;
        result = 31 * result + (target != null ? target.hashCode() : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append(String.format( //
                "(%3$s) %1$d, %2$d", //
                getSource().id(), getTarget().id(), type));
        sb.append("}");
        return sb.toString();
    }
}
