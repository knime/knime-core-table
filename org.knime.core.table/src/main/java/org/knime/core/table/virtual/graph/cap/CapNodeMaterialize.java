package org.knime.core.table.virtual.graph.cap;

import java.util.Arrays;
import java.util.UUID;

/**
 * Represents a sink of a CAP.
 * <p>
 * A {@code CapNodeMaterialize} knows the {@code CapAccessId}s (producer-slot pairs)
 * of the {@code ReadAccess}es that should be materialized into the output {@code
 * RowWriteAccessible}, and the index of the predecessor {@code CapNode}.
 */
public class CapNodeMaterialize extends CapNode {

    private final UUID uuid;
    private final CapAccessId[] inputs;
    private final int predecessor;

    /**
     *
     * @param index index of this node in the CAP list.
     * @param uuid  the UUID of the output table
     * @param inputs input columns to be materialized
     * @param predecessor index of the predecessor node in the CAP list.
     */
    public CapNodeMaterialize(final int index, final UUID uuid, CapAccessId[] inputs, final int predecessor) {
        super(index, CapNodeType.MATERIALIZE);
        this.uuid = uuid;
        this.inputs = inputs;
        this.predecessor = predecessor;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MATERIALIZE(");
        sb.append("uuid=").append(uuid);
        sb.append(", predecessor=").append(predecessor);
        sb.append(", inputs=").append(Arrays.toString(inputs));
        sb.append(')');
        return sb.toString();
    }

    /**
     * @return the UUID of the output table represented by this {@code CapNodeMaterialize}.
     */
    public UUID uuid() {
        return uuid;
    }

    /**
     * @return the {@code CapAccessId}s (producer-slot pairs) of the {@code ReadAccess}es
     *         that should bematerialized into the output {@code RowWriteAccessible}
     */
    public CapAccessId[] inputs() {
        return inputs;
    }

    /**
     * A {@code CapNodeMaterialize} has exactly one predecessor. Calling {@code forward()} on
     * the (instantiation of the) consumer will call {@code forward()} on the (instantiation
     * of the) predecessor.
     *
     * @return the index of the predecessor node in the CAP list.
     */
    public int predecessor() {
        return predecessor;
    }
}
