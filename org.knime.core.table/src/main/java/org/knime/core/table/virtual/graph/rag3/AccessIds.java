package org.knime.core.table.virtual.graph.rag3;

public class AccessIds {

    static class AccessId {
        private final Object producer;

        private AccessId parent;

        private final String label; // TODO (for debugging only)

        public AccessId(final Object producer, String label) {
            this.producer = producer;
            this.parent = this;
            this.label = label;
        }

        public AccessId(String label) {
            this(null, label);
        }

        public <P> P producer() {
            return (P)producer;
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
    }
}
