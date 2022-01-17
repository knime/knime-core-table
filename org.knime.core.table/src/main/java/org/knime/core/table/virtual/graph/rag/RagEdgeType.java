package org.knime.core.table.virtual.graph.rag;

public enum RagEdgeType {
    /**
     * <em>Spec Edges</em> reflect {@code TableTransform.getPrecedingTransforms()}
     * relations from the {@code VirtualTable}.
     * <p>
     * Edge direction is from data producer to data consumer, i.e., from source table
     * to resulting virtual table.
     */
    SPEC,

    /**
     * <em>Data Dependency Edges</em> link the node producing a particular access to
     * nodes consuming that access.
     * <p>
     * Edge direction is from producer to consumer.
     */
    DATA,

    /**
     * <em>Execution Dependency Edges</em> link nodes that depend on each other for
     * executing forward steps in order to determine that everybody is on the same row
     * (without looking at produced/consumed data). For example, to forward the CONSUMER
     * node, all SOURCE nodes have to forward (at least once) and all ROWFILTERs have to pass.
     * <p>
     * Edge direction is from executes-earlier to executes-later.
     * <p>
     * TODO: Find a better name! Mapping operations also need to execute,
     *       but they are not linked by EXEC edges.
     */
    EXEC,

    /**
     * <em>Execution Ordering Edges</em> form the partial order in which nodes need to
     * be executed, where "execute" means forwarding cursors, computing map results,
     * testing filters, etc, depending on the node type.
     */
    ORDER,

    /**
     * For visualization and debugging only...
     */
    FLATTENED_ORDER,
}
