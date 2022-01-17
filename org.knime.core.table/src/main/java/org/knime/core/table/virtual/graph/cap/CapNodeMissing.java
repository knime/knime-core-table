package org.knime.core.table.virtual.graph.cap;

import java.util.List;

import org.knime.core.table.schema.DataSpec;

/**
 * Represents a special node in the CAP which represents columns with missing data.
 * It knows the {@code DataSpec}s of all missing-data columns to produce.
 * <p>
 * A {@code CapNodeMissing} has no predecessor and no other node has it as a predecessor.
 * Therefore, {@code forward()} will never be called on a {@code CapNodeMissing}.
 */
public class CapNodeMissing extends CapNode {

    private final List<DataSpec> missingValueSpecs;

    public CapNodeMissing(final int index, List<DataSpec> missingValueSpecs) {
        super(index, CapNodeType.MISSING);
        this.missingValueSpecs = missingValueSpecs;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("MISSING(");
        sb.append("missingValueSpecs==").append(missingValueSpecs);
        sb.append(')');
        return sb.toString();
    }

    /**
     * @return the {@code DataSpec}s of all missing-data columns to produce, ordered by slot index.
     */
    public List<DataSpec> missingValueSpecs() {
        return missingValueSpecs;
    }
}
