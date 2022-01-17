package org.knime.core.table.virtual.graph.rag;

import org.knime.core.table.virtual.spec.TableTransformSpec;

/**
 * Artificial {@link TableTransformSpec} that represents the "output" of a
 * VirtualTable. Is created when the RagGraph is build.
 */
public class ConsumerTransformSpec implements TableTransformSpec {

    @Override
    public String toString() {
        return "Consumer";
    }
}
