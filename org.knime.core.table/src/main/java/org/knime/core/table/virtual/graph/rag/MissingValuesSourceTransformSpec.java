package org.knime.core.table.virtual.graph.rag;

import java.util.List;

import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.traits.DataTraits;
import org.knime.core.table.virtual.spec.TableTransformSpec;

/**
 * Artificial {@link TableTransformSpec} that represents a synthetic source that
 * provides missing value accesses. Is created when the RagGraph is build.
 */
public class MissingValuesSourceTransformSpec implements TableTransformSpec {

    private final List<Column> missingValueSpecs;

    public record Column(DataSpec spec, DataTraits traits) {
    }

    /**
     * @param missingValueSpecs the {@code DataSpec}s of all missing-values columns.
     *                          This is just referenced here, and constructed and filled elsewhere.
     */
    public MissingValuesSourceTransformSpec(final List<Column> missingValueSpecs) {
        this.missingValueSpecs = missingValueSpecs;
    }

    /**
     * Get the {@code DataSpec}s of all columns of the (singleton) missing-values source.
     */
    public List<Column> getMissingValueSpecs() {
        return missingValueSpecs;
    }

    @Override
    public String toString() {
        return "MissingValuesSource";
    }
}
