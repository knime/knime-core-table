package org.knime.core.table.virtual.graph.cap;

import org.knime.core.table.access.ReadAccess;

/**
 * Identifies a particular {@link ReadAccess} by the producer {@code CapNode} and
 * the slot index in the producer outputs.
 * <p>
 * Inputs and outputs may be sparsely populated with respect to column index. For
 * example, if certain columns of a source table are never used in a virtual table
 * construction, the output {@code CapAccessId}s of the {@code SOURCE} node will have
 * "holes" at the respective column indices.
 * <p>
 * The index of an access in the {@code CapAccessId} collection is called "slot
 * index". That is, the {@code CapAccessId} at slot index 0 is the smallest
 * "non-hole" column index.
 */
public final class CapAccessId {

    private final CapNode producer;

    private final int slot;

    public CapAccessId(final CapNode producer, final int slot) {
        this.producer = producer;
        this.slot = slot;
    }

    public CapNode producer() {
        return producer;
    }

    public int slot() {
        return slot;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof CapAccessId))
            return false;

        final CapAccessId other = (CapAccessId)o;
        return slot == other.slot && producer.equals(other.producer);
    }

    @Override
    public int hashCode() {
        return 31 * producer.hashCode() + slot;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("(");
        sb.append(producer.index());
        sb.append("::").append(slot);
        sb.append(')');
        return sb.toString();
    }
}
