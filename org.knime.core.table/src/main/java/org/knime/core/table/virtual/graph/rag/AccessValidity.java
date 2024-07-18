package org.knime.core.table.virtual.graph.rag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * TODO (TP) javadoc
 */
public class AccessValidity {// TODO (TP) move to separate file

    private final RagNode producer;

    // set of accesses with this validity (no duplicates)
    private final List<AccessId> consumers = new ArrayList<>();

    private final List<AccessId> unmodifiableConsumers = Collections.unmodifiableList(consumers);

    public AccessValidity(final RagNode producer) {
        this.producer = producer;
    }

    public RagNode getProducer() {
        return producer;
    }

    public List<AccessId> getConsumers() {
        return unmodifiableConsumers;
    }

    public void addConsumer(AccessId accessId) {
        if (!consumers.contains(accessId))
            consumers.add(accessId);
    }

    public void replaceInConsumersWith(AccessValidity validity) {
        consumers.forEach(id -> id.setValidity(validity));
        consumers.clear();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AccessValidity{");
        sb.append("producer=").append(producer);
        sb.append(", consumers=").append(consumers);
        sb.append('}');
        return sb.toString();
    }
}
