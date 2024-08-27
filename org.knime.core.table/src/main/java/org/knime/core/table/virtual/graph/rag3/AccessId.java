package org.knime.core.table.virtual.graph.rag3;

import org.knime.core.table.virtual.graph.rag3.SpecGraph.TableTransformGraph.Node;

class AccessId {

    record Producer(Node node, int index) {
    }

    private final Producer producer;

    // parent for union-find
    private AccessId parent;

    /**
     * Label is for used for {@code toString()} only, and shouldn't be relied on
     * for anything else.
     */
    private final String label;

    /**
     * {@code producer} may be {@code null}, for example if this {@code
     * AccessId} is created for a node input that is not yet linked to the
     * corresponding node output.
     *
     * @param producer the Node that produces this access
     * @param label
     */
    public AccessId(final Producer producer, String label) {
        this.producer = producer;
        this.parent = this;
        this.label = label;
    }

    public Producer producer() {
        return producer;
    }

    public AccessId find() {
        if (parent != this) {
            final var p = parent.find();
            if (parent != p) {
                parent = p;
            }
        }
        return parent;
    }

    public void union(final AccessId other) {
        parent = other.find();
    }

    @Override
    public String toString() {
        return label + (parent == this ? "" : "->" + find());
    }

    /**
     * Label is for used for {@code toString()} only, and shouldn't be relied on
     * for anything else.
     */
    String label() {
        return label;
    }
}
