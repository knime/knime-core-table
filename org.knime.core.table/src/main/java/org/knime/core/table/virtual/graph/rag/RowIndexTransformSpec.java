package org.knime.core.table.virtual.graph.rag;

import org.knime.core.table.virtual.spec.TableTransformSpec;

/**
 * Artificial {@link TableTransformSpec} that is inserted into the RagGraph at
 * points where row indices should be tracked. It is created when the RagGraph
 * is build.
 */
public class RowIndexTransformSpec implements TableTransformSpec {

    @Override
    public String toString() {
        return "RowIndex";
    }
}
