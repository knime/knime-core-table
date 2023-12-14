package org.knime.core.table.virtual.graph.rag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.knime.core.table.access.ReadAccess;

/**
 * {@code AccessId} represents a particular {@link ReadAccess}, identified by a
 * producer {@link RagNode} and the (output) column index in that node. {@code
 * AccessId} also keeps track of all consumers ({@link RagNode}s that use the access).
 */
public final class AccessId {

    private final RagNode producer;

    // set of nodes consuming the access (no duplicates)
    private final List<RagNode> consumers = new ArrayList<>();

    private final List<RagNode> unmodifiableConsumers = Collections.unmodifiableList(consumers);

    private final int columnIndex;

    public AccessId(final RagNode producer, final int columnIndex) {
        this.producer = producer;
        this.columnIndex = columnIndex;
    }

    public RagNode getProducer() {
        return producer;
    }

    /**
     * Get the (output) column index of this access in the producer node.
     */
    public int getColumnIndex() {
        return columnIndex;
    }

    public List<RagNode> getConsumers() {
        return unmodifiableConsumers;
    }

    public void addConsumer(RagNode node) {
        if (!consumers.contains(node))
            consumers.add(node);
    }

    /**
     * For each {@link #getConsumers() consumer} of this {@code AccessId}:
     * replace {@code this} with {@code newId} in the inputs of {@code
     * consumer}, add {@code consumer} to the consumers of {@code newId}.
     * Finally, clear the consumers of this {@code AccessId}.
     *
     * @param newId
     * @return {@code true} if any replacement was made (that is, unless {@link #getConsumers()} is empty).
     */
    public boolean replaceInConsumersWith(final AccessId newId) {
        if (consumers.isEmpty())
            return false;

        for (RagNode consumer : consumers) {
            consumer.replaceInput(this, newId);
            newId.addConsumer(consumer);
        }
        consumers.clear();
        return true;
    }

    public void removeConsumer(RagNode node) {
        consumers.remove(node);
    }

//    @Override
//    public String toString() {
//        final StringBuilder sb = new StringBuilder("AccessId{");
//        sb.append("producer=<").append(producer.id).append(">");
//        sb.append(", columnIndex=").append(columnIndex);
//        sb.append('}');
//        return sb.toString();
//    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("<").append(producer.id()).append(">:");
        sb.append(columnIndex);
        sb.append('}');
        return sb.toString();
    }

}
