package org.knime.core.table.virtual.graph.rag;

import org.knime.core.table.virtual.spec.TableTransformSpec;

/**
 * Artificial {@link TableTransformSpec} that represents one input table of a
 * CONCATENATE operation Is created when the RagGraph is build.
 */
public class WrapperTransformSpec implements TableTransformSpec {

    @Override
    public String toString() {
        return "Wrapper";
    }
}
