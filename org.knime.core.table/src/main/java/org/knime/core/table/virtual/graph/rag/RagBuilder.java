package org.knime.core.table.virtual.graph.rag;

import static org.knime.core.table.virtual.graph.rag.RagEdgeType.DATA;
import static org.knime.core.table.virtual.graph.rag.RagEdgeType.EXEC;
import static org.knime.core.table.virtual.graph.rag.RagEdgeType.ORDER;
import static org.knime.core.table.virtual.graph.rag.RagEdgeType.SPEC;
import static org.knime.core.table.virtual.graph.rag.RagGraphUtils.topologicalSort;
import static org.knime.core.table.virtual.graph.rag.RagNodeType.APPEND;
import static org.knime.core.table.virtual.graph.rag.RagNodeType.CONCATENATE;
import static org.knime.core.table.virtual.graph.rag.RagNodeType.CONSUMER;
import static org.knime.core.table.virtual.graph.rag.RagNodeType.MAP;
import static org.knime.core.table.virtual.graph.rag.RagNodeType.MATERIALIZE;
import static org.knime.core.table.virtual.graph.rag.RagNodeType.MISSING;
import static org.knime.core.table.virtual.graph.rag.RagNodeType.ROWFILTER;
import static org.knime.core.table.virtual.graph.rag.RagNodeType.SLICE;
import static org.knime.core.table.virtual.graph.rag.RagNodeType.SOURCE;
import static org.knime.core.table.virtual.graph.rag.RagNodeType.WRAPPER;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.LongBinaryOperator;

import org.knime.core.table.cursor.Cursors;
import org.knime.core.table.row.Selection.RowRangeSelection;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.DefaultColumnarSchema;
import org.knime.core.table.schema.traits.DataTraits;
import org.knime.core.table.virtual.TableTransform;
import org.knime.core.table.virtual.VirtualTable;
import org.knime.core.table.virtual.graph.cap.CapBuilder;
import org.knime.core.table.virtual.spec.AppendMissingValuesTransformSpec;
import org.knime.core.table.virtual.spec.MapTransformSpec;
import org.knime.core.table.virtual.spec.MaterializeTransformSpec;
import org.knime.core.table.virtual.spec.RowFilterTransformSpec;
import org.knime.core.table.virtual.spec.SelectColumnsTransformSpec;
import org.knime.core.table.virtual.spec.SliceTransformSpec;
import org.knime.core.table.virtual.spec.SourceTransformSpec;
import org.knime.core.table.virtual.spec.TableTransformSpec;

/**
 * Create a {@code RagGraph} from a {@code VirtualTable} spec.
 *
 * <ol>
 * <li>{@link #buildSpec(TableTransform)}:
 *      Build RagGraph nodes for each TableTransform in the given VirtualTable, and SPEC
 *      edges between them. Set numColumns for each node.</li>
 * <li>{@link #traceAccesses()}:
 *      Trace each access from its consumer to its producer, starting with accesses
 *      consumed by the root node, and walking along SPEC edges.</li>
 * <li>{@link #traceExec()}:
 *      Recursively trace along reverse SPEC edges to insert EXEC edges between
 *      executable nodes, starting in turn from each node of the graph.</li>
 * <li>{@link #optimize()}:
 *      Apply local optimizations to the graph. (At the moment: merging consecutive
 *      SLICE nodes, and eliminating certain APPEND nodes).</li>
 * <li>{@link #createExecutionOrderingEdges()}:
 *      Create ORDER edges by taking the union of DATA and EXEC dependency edges and
 *      doing a transitive reduction.</li>
 * </ol>
 */
public class RagBuilder {

    public static final Function<List<RagNode>, RagNode> DEFAULT_POLICY = nodes -> {
        nodes.sort(Comparator.comparingInt(node -> switch (node.type()) {
            default -> 0;
            case SLICE -> 1;
            case ROWFILTER -> 2;
            case APPEND -> 3;
            case CONCATENATE -> 4;
            case MAP -> 5;
        }));
        return nodes.get(0);
    };

    public static List<RagNode> createOrderedRag(final VirtualTable table) {
        return createOrderedRag(table.getProducingTransform(), DEFAULT_POLICY);
    }

    public static List<RagNode> createOrderedRag(
            final VirtualTable table,
            final Function<List<RagNode>, RagNode> policy) {
        return createOrderedRag(table.getProducingTransform(), policy);
    }

    public static List<RagNode> createOrderedRag(final TableTransform tableTransform) {
        return createOrderedRag(tableTransform, DEFAULT_POLICY);
    }

    public static List<RagNode> createOrderedRag(
            final TableTransform tableTransform,
            final Function<List<RagNode>, RagNode> policy) {

        final RagBuilder specs = new RagBuilder();
        specs.buildSpec(tableTransform);
        specs.traceAccesses();
        specs.traceExec();
        specs.optimize();
        specs.createExecutionOrderingEdges();
        specs.removeWrapperNodes();

        final List<RagNode> order = specs.getFlattenedExecutionOrder(policy);

        // If we need missing value columns, prepend the MISSING source to the execution order
        // (it should not be connected by ORDER edges)
        // TODO: Avoid this special case logic?
        final RagNode missingValuesSource = specs.graph.getMissingValuesSource();
        if ( !missingValuesSource.getOutputs().isEmpty() ) {
            order.add(0, missingValuesSource);
        }

        return order;
    }

    public static ColumnarSchema createSchema(final List<RagNode> orderedRag) {
        final RagNode node = orderedRag.get(orderedRag.size() - 1);
        return createSinkSchema(node);
    }

    public static Map<UUID, ColumnarSchema> getSourceAndSinkSchemas(final List<RagNode> orderedRag) {
        Map<UUID, ColumnarSchema> schemas = new HashMap<>();
        for (RagNode node : orderedRag) {
            if (node.type() == SOURCE) {
                SourceTransformSpec spec = node.getTransformSpec();
                schemas.put(spec.getSourceIdentifier(), spec.getSchema());
            } else if (node.type() == MATERIALIZE) {
                MaterializeTransformSpec spec = node.getTransformSpec();
                schemas.put(spec.getSinkIdentifier(), createSinkSchema(node));
            }
        }
        return schemas;
    }

    private static ColumnarSchema createSinkSchema(final RagNode node) {
        if (!sinkNodeTypes.contains(node.type())) {
            throw new IllegalArgumentException(
                    "Unexpected RagNode type " + node.type() + ".");
        }
        final List<DataSpec> columnSpecs = new ArrayList<>();
        final List<DataTraits> columnTraits = new ArrayList<>();
        for (AccessId input : node.getInputs()) {
            columnSpecs.add(getSpec(input));
            columnTraits.add(getTraits(input));
        }
        return new DefaultColumnarSchema(columnSpecs, columnTraits);
    }

    private static DataSpec getSpec(final AccessId accessId) {
        final RagNode producer = accessId.getProducer();
        switch (producer.type()) {
            case SOURCE: {
                final SourceTransformSpec spec = producer.getTransformSpec();
                return spec.getSchema().getSpec(accessId.getColumnIndex());
            }
            case MISSING: {
                final MissingValuesSourceTransformSpec spec = producer.getTransformSpec();
                return spec.getMissingValueSpecs().get(accessId.getColumnIndex()).spec();
            }
            case APPEND:
            case CONCATENATE: {
                final int slot = producer.getOutputs().slotIndexOf(accessId);
                return getSpec(producer.getInputs(0).getAtSlot(slot));
            }
            case MAP: {
                final MapTransformSpec spec = producer.getTransformSpec();
                return spec.getMapperFactory().getOutputSchema().getSpec(accessId.getColumnIndex());
            }
            default:
                throw new IllegalArgumentException("unexpected node type " + producer.type());
        }
    }

    private static DataTraits getTraits(final AccessId accessId) {
        var producer = accessId.getProducer();
        switch (producer.type()) {
            case SOURCE: {
                final SourceTransformSpec spec = producer.getTransformSpec();
                return spec.getSchema().getTraits(accessId.getColumnIndex());
            }
            case MISSING: {
                final MissingValuesSourceTransformSpec spec = producer.getTransformSpec();
                return spec.getMissingValueSpecs().get(accessId.getColumnIndex()).traits();
            }
            case APPEND:
            case CONCATENATE: {
                final int slot = producer.getOutputs().slotIndexOf(accessId);
                return getTraits(producer.getInputs(0).getAtSlot(slot));
            }
            case MAP: {
                final MapTransformSpec spec = producer.getTransformSpec();
                return spec.getMapperFactory().getOutputSchema().getTraits(accessId.getColumnIndex());
            }
            default:
                throw new IllegalArgumentException("unexpected node type " + producer.type());
        }
    }

    /**
     * Returns {@code true} if the given linearized {@code RagGraph} supports {@code
     * LookaheadCursor}s without additional prefetching and buffering (see {@link
     * Cursors#toLookahead}).
     * <p>
     * This is possible, if all sources provide {@code LookaheadCursor}s and there are
     * no row-filters (or other nodes that would destroy lookahead capability.)
     *
     * @param orderedRag a linearized {@code RagGraph}
     * @return {@code true} if the {@code orderedRag} supports {@code LookaheadCursor}s
     */
    public static boolean supportsLookahead(final List<RagNode> orderedRag) {
        final RagNode node = orderedRag.get(orderedRag.size() - 1);
        if (!sinkNodeTypes.contains(node.type())) {
            throw new IllegalArgumentException();
        }
        return supportsLookahead(node);
    }

    private static boolean supportsLookahead(final RagNode node) {
        switch (node.type()) {
            case SOURCE: {
                final SourceTransformSpec spec = node.getTransformSpec();
                return spec.getProperties().supportsLookahead();
            }
            case ROWFILTER: {
                return false;
            }
            case SLICE:
            case APPEND:
            case CONCATENATE:
            case CONSUMER:
            case MATERIALIZE: {
                for (RagNode predecessor : node.predecessors(EXEC)) {
                    if (!supportsLookahead(predecessor)) {
                        return false;
                    }
                }
                return true;
            }
            case MISSING:
            case MAP:
            case COLFILTER:
            case APPENDMISSING:
                throw new IllegalArgumentException(
                        "Unexpected RagNode type " + node.type() + ".");
            default:
                throw new IllegalStateException("Unexpected value: " + node.type());
        }
    }

    /**
     * Returns the number of rows of the given linearized {@code RagGraph}, or a negative value
     * if the number of rows is unknown.
     *
     * @param orderedRag a linearized {@code RagGraph}
     * @return number of rows at the consumer node of the {@code orderedRag}
     */
    public static long numRows(final List<RagNode> orderedRag) {
        return numRows(orderedRag, true);
    }

    /**
     * Recursively trace along reverse EXEC edges to determine number of rows.
     */
    private static long numRows(final RagNode node) {
        return new NumRows(false).numRows(node);
        // TODO (TP) Look at usages. Should do better than recomputing everything all the time.
    }

    private static long numRows(final List<RagNode> orderedRag, final boolean setNodeNumRows) {
        final RagNode node = orderedRag.get(orderedRag.size() - 1);
        if (!sinkNodeTypes.contains(node.type())) {
            throw new IllegalArgumentException();
        }
        return new NumRows(setNodeNumRows).numRows(node);
    }

    private static class NumRows
    {
        /**
         * @param setNodeNumRows if {@code true}, {@link RagNode#setNumRows store} numRows in each visited node.
         */
        NumRows(final boolean setNodeNumRows) {
            this.setNodeNumRows = setNodeNumRows;
        }

        private final boolean setNodeNumRows;

        private final Map<RagNode, Long> cache = new HashMap<>();

        private long cache(final RagNode key, final long value) {
            cache.put(key, value);
            if (setNodeNumRows) {
                key.setNumRows(value);
            }
            return value;
        }

        /**
         * Recursively trace along reverse EXEC edges to determine number of rows.
         */
        long numRows(final RagNode node) {

            final Long cachedNumRows = cache.get(node);
            if (cachedNumRows != null) {
                return cachedNumRows;
            }

            switch (node.type()) {
                case SOURCE: {
                    final SourceTransformSpec spec = node.getTransformSpec();
                    long numRows = spec.getProperties().numRows();
                    if (numRows < 0) {
                        return cache(node, -1);
                    }
                    final RowRangeSelection range = spec.getRowRange();
                    if (range.allSelected()) {
                        return cache(node, numRows);
                    } else {
                        final long from = range.fromIndex();
                        final long to = range.toIndex();
                        return cache(node, Math.max(0, Math.min(numRows, to) - from));
                    }
                }
                case SLICE: {
                    final SliceTransformSpec spec = node.getTransformSpec();
                    final long from = spec.getRowRangeSelection().fromIndex();
                    final long to = spec.getRowRangeSelection().toIndex();
                    // If any predecessor doesn't know its size, the size of this node is also unknown.
                    // Otherwise, the size of this node is max of its predecessors.
                    final long s = accPredecessorNumRows(node, Math::max);
                    return cache(node, s < 0 ? s : Math.max(0, Math.min(s, to) - from));
                }
                case CONSUMER:
                case MATERIALIZE:
                case WRAPPER:
                case APPEND: {
                    // If any predecessor doesn't know its size, the size of this node is also unknown.
                    // Otherwise, the size of this node is max of its predecessors.
                    // If any predecessor doesn't know its size, the size of this node is also unknown.
                    // Otherwise, the size of this node is max of its predecessors.
                    return cache(node, accPredecessorNumRows(node, Math::max));
                }
                case CONCATENATE: {
                    // If any predecessor doesn't know its size, the size of this node is also unknown.
                    // Otherwise, the size of this is the sum of its predecessors.
                    return cache(node, accPredecessorNumRows(node, Long::sum));
                }
                case ROWFILTER:
                    return cache(node, -1);
                case MISSING:
                case APPENDMISSING:
                case COLFILTER:
                case MAP:
                case IDENTITY:
                    throw new IllegalArgumentException(
                            "Unexpected RagNode type " + node.type() + ".");
                default:
                    throw new IllegalStateException("Unexpected value: " + node.type());
            }
        }

        /**
         * Accumulate numRows of EXEC predecessors of {@code node}.
         * Returns -1, if at least one predecessor doesn't know its numRows.
         * Returns 0 if there are no predecessors.
         */
        private long accPredecessorNumRows(final RagNode node, final LongBinaryOperator acc) {
            // If any predecessor doesn't know its size, the size of this node
            // is also unknown (return -1).
            // Otherwise, the size of this node is the accumulated size of its
            // predecessors (via the specified acc operator).
            long size = 0;
            for (final RagNode predecessor : node.predecessors(EXEC)) {
                final long s = numRows(predecessor);
                if ( s < 0 ) {
                    return -1;
                }
                size = acc.applyAsLong(size, s);
            }
            return size;
        }
    }




    final RagGraph graph = new RagGraph();

    private final MissingValueColumns missingValueColumns = new MissingValueColumns();

    /**
     * Executable nodes are linked by EXEC edges.
     * <p>
     * A node is "executable" if it can be forwarded. Executable nodes
     * depend on each other for executing forward steps in order to determine that
     * everybody is on the same row (without looking at produced/consumed data).
     */
    // TODO: move to RagNodeType
    public static final EnumSet<RagNodeType> executableNodeTypes = EnumSet.of(APPEND, SOURCE, CONCATENATE, SLICE, ROWFILTER, WRAPPER);

    /**
     * Spawning nodes create new sub-trees of their SPEC predecessor nodes. This
     * prevents (incorrect) fork-join type fusion of branches when forwarding structure
     * diverges.
     */
    private static final EnumSet<RagNodeType> spawningNodeTypes = EnumSet.of(SLICE, CONCATENATE);

    /**
     * Sink nodes are consumer-like endpoints.
     */
    private static final EnumSet<RagNodeType> sinkNodeTypes = EnumSet.of(CONSUMER, MATERIALIZE);

    RagBuilder() {
    }


    // --------------------------------------------------------------------
    // buildSpec(VirtualTable)

    /**
     * Build RagGraph nodes for each TableTransform in the given VirtualTable, and SPEC
     * edges between them. Set numColumns for each node.
     */
    void buildSpec(final TableTransform tableTransform) {

        final TableTransform missingValuesTransform = new TableTransform(//
                Collections.emptyList(),//
                new MissingValuesSourceTransformSpec(missingValueColumns.unmodifiable));
        graph.setMissingValuesSource(graph.addNode(missingValuesTransform));

        // For now, we expect only a single output table.
        // Either the final transform is already a MaterializeTransformSpec, in
        // which case this becomes the root node (a MATERIALIZE node).
        // Or, the final transform just describes some other VirtualTable, in which
        // case, an artificial CONSUMER node is appended and becomes the root.
        if ( tableTransform.getSpec() instanceof MaterializeTransformSpec) {
            final RagNode root = createNodes(tableTransform, new HashMap<>());
            graph.setRoot(root);
        } else {
            final var consumerTransform = new TableTransform(//
                    List.of(tableTransform),//
                    new ConsumerTransformSpec());
            final RagNode root = createNodes(consumerTransform, new HashMap<>());
            graph.setRoot(root);
        }

        buildNumColumns();
    }

    /**
     * Create a Node for the given TableTransform, and recursively create Nodes (and
     * Edges) for its precedingTransforms.
     *
     * @param transform
     * @param nodeLookup
     *          Links TableTransforms to already existing Nodes. This is used to handle
     *          fork-join-type structures.
     * @return
     */
    // TODO: make iterative instead of recursive
    private RagNode createNodes(final TableTransform transform, final Map<TableTransform, RagNode> nodeLookup) {
        RagNode node = nodeLookup.get(transform);
        if (node == null) {
            // create node for current TableTransform
            node = graph.addNode(transform);
            nodeLookup.put(transform, node);

            // create and link nodes for preceding TableTransforms
            for (final TableTransform t : transform.getPrecedingTransforms()) {
                Map<TableTransform, RagNode> lookup = spawningNodeTypes.contains(node.type()) ? new HashMap<>() : nodeLookup;
                final RagNode input = createNodes(t, lookup);
                if (node.type() == CONCATENATE) {
                    final var wrapTransform = new TableTransform(Collections.emptyList(), new WrapperTransformSpec());
                    final RagNode wrap = graph.addNode(wrapTransform);
                    graph.addEdge(input, wrap, SPEC);
                    graph.addEdge(wrap, node, SPEC);
                } else {
                    graph.addEdge(input, node, SPEC);
                }
            }
        }
        return node;
    }

    /**
     * Starting at SOURCE nodes, find number of columns for each node.
     */
    private void buildNumColumns() {
        for (final RagNode node : topologicalSort(graph, SPEC)) {
            final TableTransformSpec spec = node.getTransformSpec();
            int numColumns = 0;
            switch (node.type()) {
                case SOURCE:
                    numColumns = ((SourceTransformSpec)spec).getSchema().numColumns();
                    break;
                case CONSUMER:
                case MATERIALIZE:
                case SLICE:
                case ROWFILTER:
                case IDENTITY:
                case WRAPPER:
                    numColumns = node.predecessor(SPEC).numColumns();
                    break;
                case CONCATENATE:
                    numColumns = node.predecessors(SPEC).get(0).numColumns();
                    break;
                case APPEND:
                    numColumns = node.predecessors(SPEC).stream().mapToInt(RagNode::numColumns).sum();
                    break;
                case APPENDMISSING:
                    numColumns = node.predecessor(SPEC).numColumns()//
                            + ((AppendMissingValuesTransformSpec)spec).getAppendedSchema().numColumns();
                    break;
                case COLFILTER:
                    numColumns = ((SelectColumnsTransformSpec)spec).getColumnSelection().length;
                    break;
                case MAP:
                    numColumns = ((MapTransformSpec)spec).getMapperFactory().getOutputSchema().numColumns();
                    break;
                case MISSING:
                    break;
                default:
                    throw new IllegalArgumentException("Unexpected value: " + node.type());
            }
            node.setNumColumns(numColumns);
        }
    }



    // --------------------------------------------------------------------
    // traceAccesses()

    /**
     * Trace each access from its consumer to its producer, starting with accesses
     * consumed by the root node, and walking along SPEC edges.
     * <p>
     * TODO Instead of special-casing MissingValuesSource in {@link CapBuilder}), should we
     *      add an edge to every SOURCE such that MISSING comes first in execution order?
     */
    void traceAccesses() {
        // mark is used to mark MAP nodes that already have traced their incoming accesses
        graph.nodes(MAP).forEach(node -> node.setMark(0));

        final RagNode root = graph.getRoot();
        for (int i = 0; i < root.numColumns(); i++) {
            traceAndLinkAccess(i, root);
        }

        graph.nodes(ROWFILTER).forEach(node -> {
            final int numInputColumns = node.<RowFilterTransformSpec>getTransformSpec().getColumnSelection().length;
            for (int i = 0; i < numInputColumns; i++) {
                traceAndLinkAccess(i, node);
            }
        });

        final List<RagEdge> edges = new ArrayList<>(graph.getMissingValuesSource().outgoingEdges(DATA));
        edges.forEach(graph::remove);
    }

    /**
     * Trace the access <em><b>consumed</b></em> by {@code node} at column {@code i} to
     * its producer. This will build up an AccessId at the producer and insert a DATA
     * dependency edges linking the producer to this {@code node}.
     */
    private void traceAndLinkAccess(final int i, final RagNode node) {
        final int numPredecessors = node.getInputssArray().length;
        final AccessId[] accessIds = new AccessId[numPredecessors];
        // N.B. accessIds[] is indexed by predecessor, not column! That is, accessIds[p]
        // holds i-th column for p-th predecessor.

        switch (node.type())
        {
            case APPEND: {
                final List<RagNode> predecessors = node.predecessors(SPEC);
                int startCol = 0;
                for (final RagNode predecessor : predecessors) {
                    final int nextStartCol = startCol + predecessor.numColumns();
                    if (i < nextStartCol) {
                        accessIds[0] = traceAccess(i - startCol, predecessor);
                        break;
                    }
                    startCol = nextStartCol;
                }
                break;
            }
            case CONCATENATE: {
                final List<RagNode> predecessors = node.predecessors(SPEC);
                for (int j = 0; j < predecessors.size(); j++) {
                    accessIds[j] = traceAccess(i, predecessors.get(j));
                }
                break;
            }
            case MAP: {
                final int[] selection = ((MapTransformSpec)node.getTransformSpec()).getColumnSelection();
                accessIds[0] = traceAccess(selection[i], node.predecessor(SPEC));
                break;
            }
            case ROWFILTER: {
                final int[] selection = ((RowFilterTransformSpec)node.getTransformSpec()).getColumnSelection();
                accessIds[0] = traceAccess(selection[i], node.predecessor(SPEC));
                break;
            }
            case WRAPPER:
            case CONSUMER:
            case MATERIALIZE: {
                accessIds[0] = traceAccess(i, node.predecessor(SPEC));
                break;
            }
            case SOURCE:
            case SLICE:
            case APPENDMISSING:
            case COLFILTER:
                throw new IllegalArgumentException("Unexpected RagNode type " + node.type() + ".");
            default:
                throw new IllegalStateException("Unexpected value: " + node.type());
        }
        for (int j = 0; j < accessIds.length; j++) {
            final AccessId accessId = accessIds[j];

            accessId.addConsumer(node);
            node.getInputs(j).putAtColumnIndex(accessId, i);

            // add Data Dependency Edge (if it doesn't exist yet)
            graph.getOrAddEdge(accessId.getProducer(), node, DATA);
        }
    }

    /**
     * Recursively trace the access <em><b>output</b></em> by {@code node} at column
     * {@code i} to its producer.
     */
    private AccessId traceAccess(final int i, final RagNode node) {
        final TableTransformSpec spec = node.getTransformSpec();
        switch (node.type()) {
            case SOURCE:
                return node.getOrCreateOutput(i);
            case MAP:
                // N.B. This would be repeated as we visit each outgoing column i. However, we only
                // need to trace the map input columns once. Therefore, after the input-tracing has
                // happened the node is marked, and the tracing is not repeated.
                if (node.getMark() == 0) {
                    node.setMark(1);
                    final int numInputColumns = ((MapTransformSpec)spec).getColumnSelection().length;
                    for (int j = 0; j < numInputColumns; j++) {
                        traceAndLinkAccess(j, node);
                    }
                }
                return node.getOrCreateOutput(i);
            case ROWFILTER:
            case SLICE:
            case IDENTITY:
                return traceAccess(i, node.predecessor(SPEC));
            case APPEND:
            case CONCATENATE:
            case WRAPPER:
                traceAndLinkAccess(i, node);
                return node.getOrCreateOutput(i);
            case APPENDMISSING:
                final int numPredecessorColumns = node.predecessor(SPEC).numColumns();
                if (i < numPredecessorColumns) {
                    return traceAccess(i, node.predecessor(SPEC));
                } else {
                    // get the DataSpec of output i
                    final ColumnarSchema appendedSchema = ((AppendMissingValuesTransformSpec)spec).getAppendedSchema();
                    final DataSpec dataSpec = appendedSchema.getSpec(i - numPredecessorColumns);
                    final DataTraits traits = appendedSchema.getTraits(i - numPredecessorColumns);
                    // get/add the index of the missing AccessId for that DataSpec
                    // and get/add missingValuesSource output AccessId for that index
                    final RagNode missingValuesSource = graph.getMissingValuesSource();
                    return missingValuesSource.getOrCreateOutput(missingValueColumns.getOrAdd(dataSpec, traits));
                }
            case COLFILTER:
                final int[] selection = ((SelectColumnsTransformSpec)spec).getColumnSelection();
                return traceAccess(selection[i], node.predecessor(SPEC));
            case CONSUMER:
            case MATERIALIZE:
            case MISSING:
                throw new IllegalArgumentException("Unexpected RagNode type " + node.type() + ".");
            default:
                throw new IllegalStateException("Unexpected value: " + node.type());
        }
    }



    // --------------------------------------------------------------------
    // traceExec()

    /**
     * Recursively trace along reverse SPEC edges to insert EXEC edges between
     * executable nodes, starting in turn from each node of the graph.
     */
    void traceExec() {
        for (final RagNode node : graph.nodes()) {
            traceExec(node);
        }
    }

    /**
     * Recursively trace along reverse SPEC edges to insert EXEC edges between
     * executable nodes, starting from the specified {@code node}.
     */
    private void traceExec(final RagNode node) {
        switch (node.type()) {
            case SOURCE:
            case COLFILTER:
            case MISSING:
            case APPENDMISSING:
            case MAP:
            case IDENTITY:
                // do nothing
                break;
            case SLICE:
            case CONCATENATE:
            case CONSUMER:
            case MATERIALIZE:
            case APPEND:
            case ROWFILTER:
            case WRAPPER:
                traceExecConsumer(node, node, false);
//                traceExecConsumer(node, node);
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + node.type());
        }
    }

    /**
     * Recursively trace along reverse SPEC edges to insert EXEC edges between
     * executable nodes. A node is executable if it can be forwarded. Executable nodes
     * depend on each other for executing forward steps in order to determine that
     * everybody is on the same row (without looking at produced/consumed data).
     * <p>
     * To make sure that ROWFILTER nodes (that occur sequentially in SPEC order)
     * can be re-ordered in EXEC, edges are inserted as follows (through
     * recursive calls of this method).
     * <p>
     * While tracing from a non-ROWFILTER {@code dest}: insert EXEC edge from
     * the first non-ROWFILTER executable node to {@code dest} if there is no
     * intermediate ROWFILTER node. Otherwise insert an EXEC edge from every
     * intermediate ROWFILTER node to {@code dest}.
     * <p>
     * While tracing from a ROWFILTER {@code dest}: insert EXEC edge from the
     * first non-ROWFILTER executable node to {@code dest}.
     *
     * @param dest    the node from which the current recursive trace started. All
     *                potentially created EXEC edges link <em>to</em> the {@code dest} node.
     * @param current the node which is currently visited along the recursive trace. An
     *                EXEC edge linking <em>from</em> the {@code current} node is created, if the
     *                {@code current} node is an executable node.
     * @param rowFilterHit whether a ROWFILTER node was already visited on the
     *                way from {@code dest} to {@code current}
     */
    private void traceExecConsumer(final RagNode dest, final RagNode current, boolean rowFilterHit) {

//      From an executable node (except RowFilter) dest: follow spec edges recursively.
//        If an executable node is hit:
//          - insert an EXEC edge (node --> dest).
//          - if node is not a RowFilter, stop tracing.
//      From a RowFilter node dest: follow spec edges recursively.
//        If a RowFilter node is hit:
//          - don't insert an edge
//          - keep on tracing
//        If an executable node (except RowFilter) is hit:
//          - insert an EXEC edge node --> dest
//          - stop tracing

        if (!dest.equals(current)) {
            final RagNodeType ctype = current.type();
            if (executableNodeTypes.contains(ctype)) {
                if (ctype == ROWFILTER) {
                    rowFilterHit = true;
                    if (dest.type() != ROWFILTER) {
                        graph.getOrAddEdge(current, dest, EXEC);
                    }
                    // continue tracing recursively
                } else {
                    if (dest.type() == ROWFILTER || !rowFilterHit) {
                        graph.getOrAddEdge(current, dest, EXEC);
                    }
                    return; // no further recursion
                }
            }
        }
        // recurse along reverse SPEC edges
        for (RagNode predecessor : current.predecessors(SPEC)) {
            traceExecConsumer(dest, predecessor, rowFilterHit);
        }
    }



    // --------------------------------------------------------------------
    // optimize()

    void optimize() {
        graph.trim();
        boolean changed = true;
        while (changed) {
            changed = false;
            changed |= eliminateAppends();
            changed |= mergeSlices();
            changed |= moveSlicesBeforeAppends();
            changed |= moveSlicesBeforeConcatenates();
            changed |= eliminateSingletonConcatenates();
            // TODO other optimizations
        }
    }



    // --------------------------------------------------------------------
    // helpers

    /**
     * For each successor (of the specified {@code edgeType}) of {@code
     * oldNode}: Replace the edge linking {@code oldNode} to successor with an
     * edge linking {@code newNode} to successor.
     */
    private void relinkSuccessorsToNewSource(final RagNode oldNode, final RagNode newNode, final RagEdgeType edgeType)
    {
        final List<RagEdge> edges = new ArrayList<>(oldNode.outgoingEdges(edgeType));
        edges.forEach(edge -> graph.replaceEdgeSource(edge, newNode));
    }

    /**
     * For each predecessor (of the specified {@code edgeType}) of {@code
     * oldNode}: Replace the edge linking predecessor to {@code oldNode} with an
     * edge linking predecessor to {@code newNode}.
     */
    private void relinkPredecessorsToNewTarget(final RagNode oldNode, final RagNode newNode, final RagEdgeType edgeType)
    {
        final List<RagEdge> edges = new ArrayList<>(oldNode.incomingEdges(edgeType));
        edges.forEach(edge -> graph.replaceEdgeTarget(edge, newNode));
    }



    // --------------------------------------------------------------------
    // eliminateSingletonConcatenates()

    boolean eliminateSingletonConcatenates() {
        for (final RagNode node : graph.nodes(CONCATENATE)) {
            if (tryEliminateConcatenate(node)) {
                return true;
            }
        }
        return false;
    }

    private boolean tryEliminateConcatenate(final RagNode concatenate) {
        final List<RagNode> predecessors = concatenate.predecessors(EXEC);
        if (predecessors.size() == 1) {
            // There is exactly one WRAPPER node linked to this CONCATENATE.
            // Both can be short-circuited and removed.
            final RagNode wrapper = predecessors.get(0);

            // Short-circuit EXEC edges from predecessors of WRAPPER to successors of CONCATENATE
            for (RagNode predecessor : wrapper.predecessors(EXEC)) {
                for (RagNode successor : concatenate.successors(EXEC)) {
                    graph.getOrAddEdge(predecessor, successor, EXEC);
                }
            }

            graph.remove(wrapper);
            graph.remove(concatenate);

            // For each output in CONCATENATE.outputs[i]:
            //   For each consumer in output.getConsumers():
            //     Find consumer.inputs[j] corresponding to output, and replace by WRAPPER.inputs[i]
            // Remove WRAPPER from the consumers of its inputs, and add short-circuited consumers instead.
            // Add new DATA edges from producers to short-circuited consumers.
            final AccessIds inputs = wrapper.getInputs();
            final AccessIds outputs = concatenate.getOutputs();
            for (int i = 0; i < outputs.size(); i++) {
                final AccessId output = outputs.getAtSlot(i);
                final AccessId input = inputs.getAtSlot(i);
                input.removeConsumer(wrapper);
                for (RagNode consumer : output.getConsumers()) {
                    if (consumer.replaceInput(output, input)) {
                        input.addConsumer(consumer);
                        if (input.getProducer().type() != MISSING) {
                            graph.getOrAddEdge(input.getProducer(), consumer, DATA);
                        }
                    }
                }
            }

            return true;
        }
        return false;
    }



    // --------------------------------------------------------------------
    // moveSliceBeforeConcatenate()

    boolean moveSlicesBeforeConcatenates() {
        for (final RagNode node : graph.nodes(SLICE)) {
            if (tryMoveSliceBeforeConcatenate(node)) {
                return true;
            }
        }
        return false;
    }

    private boolean tryMoveSliceBeforeConcatenate(final RagNode slice) {
        final List<RagNode> predecessors = slice.predecessors(EXEC);
        if (predecessors.size() == 1 && predecessors.get(0).type() == CONCATENATE) {
            final RagNode concatenate = predecessors.get(0);

            final SliceTransformSpec sliceSpec = slice.getTransformSpec();
            final RowRangeSelection sliceRange = sliceSpec.getRowRangeSelection();
            final long s0 = sliceRange.fromIndex();
            final long s1 = sliceRange.toIndex();

            // determine predecessors
            final List<RagNode> concatenated = concatenate.predecessors(EXEC);
            long r0 = 0;

            // Going through the predecessors we record what needs to be done:
            //
            // 1.) List of predecessors to keep, and respective row ranges.
            final class SlicedNode {
                final RagNode node;
                final RowRangeSelection rows;

                SlicedNode( final RagNode node, final RowRangeSelection rows ) {
                    this.node = node;
                    this.rows = rows;
                }
            }
            final List<SlicedNode> slicedConcatenated = new ArrayList<>();

            // 2.) List of predecessors (indices) to remove.
            final List<Integer> removeConcatenated = new ArrayList<>();

            for (int i = 0; i < concatenated.size(); i++) {
                final RagNode node = concatenated.get(i);
                final long r = numRows(node);
                if (r < 0) {
                    // Cannot move the SLICE because slicing into a predecessor
                    // with unknown number of rows.
                    if ( !removeConcatenated.isEmpty() ) {
                        // Although the SLICE can't be moved, we can still
                        // remove input tables that fall before the start of the
                        // sliced range,
                        pruneConcatenatePredecessors(concatenate, concatenated, removeConcatenated);
                        return true;
                    }
                    return false;
                }
                final long r1 = r0 + r;
                if (r1 <= s0) {
                    // This predecessor can be removed
                    removeConcatenated.add(i);
                } else if (r0 >= s1) {
                    // This and all following predecessors, can be removed
                    for (int j = i; j < concatenated.size(); j++) {
                        removeConcatenated.add(j);
                    }
                    break;
                } else {
                    final long t0 = Math.max(0, s0 - r0);
                    final long t1 = Math.min(r, s1 - r0);
                    if (t0 == 0 && t1 == r) {
                        // keep this predecessor (no slicing)
                        slicedConcatenated.add(new SlicedNode(node, RowRangeSelection.all()));
                    } else {
                        // keep slice(t0, t1) of this predecessor
                        slicedConcatenated.add(new SlicedNode(node, RowRangeSelection.all().retain(t0, t1)));
                    }
                }
                r0 = r1;
            }

            // use slicedConcatenated to rebuild the inputs to CONCATENATE
            if (slicedConcatenated.isEmpty()) {
                throw new IllegalStateException();
            }

            pruneConcatenatePredecessors(concatenate, concatenated, removeConcatenated);

            for (SlicedNode slicedNode : slicedConcatenated) {
                final RagNode wrapper = slicedNode.node;
                final RowRangeSelection rows = slicedNode.rows;

                if (!rows.allSelected()) {
                    // create new SLICE node
                    final TableTransform presliceTransform =
                            new TableTransform(Collections.emptyList(), new SliceTransformSpec(rows));
                    final RagNode preslice = graph.addNode(presliceTransform);

                    // insert preslice between wrapper and its EXEC predecessors
                    relinkPredecessorsToNewTarget(wrapper, preslice, EXEC);
                    graph.addEdge(preslice, wrapper, EXEC);
                }
                // Note, that if rows.allSelected() == true then we don't have
                // to do anything. wrapper just stays connected to CONCATENATE.
            }

            // short-circuit SLICE successors to CONCATENATE
            relinkSuccessorsToNewSource(slice, concatenate, EXEC);

            graph.remove(slice);
            return true;
        }
        return false;
    }

    /**
     * @param concatenate the CONCATENATE node
     * @param predecessors EXEC predecessors of {@code concatenate}
     * @param predecessorsToRemove indices of predecessors to remove
     */
    private void pruneConcatenatePredecessors(final RagNode concatenate, final List<RagNode> predecessors, final List<Integer> predecessorsToRemove) {
        final AccessIds[] inputss = new AccessIds[predecessors.size() - predecessorsToRemove.size()];
        for (int i = 0, j = 0, o = 0; i < predecessors.size(); ++i) {
            if (j < predecessorsToRemove.size() && predecessorsToRemove.get(j) == i) {
                graph.remove(predecessors.get(i));
                ++j;
            } else {
                inputss[o] = concatenate.getInputs(i);
                ++o;
            }
        }
        concatenate.setInputssArray(inputss);
    }



    // --------------------------------------------------------------------
    // moveSliceBeforeAppend()

    boolean moveSlicesBeforeAppends() {
        for (final RagNode node : graph.nodes(SLICE)) {
            if (tryMoveSliceBeforeAppend(node)) {
                return true;
            }
        }
        return false;
    }

    private boolean tryMoveSliceBeforeAppend(final RagNode slice) {
        final List<RagNode> predecessors = slice.predecessors(EXEC);
        if (predecessors.size() == 1 && predecessors.get(0).type() == APPEND) {
            final RagNode append = predecessors.get(0);

            // remove "slice":
            // short-circuit "append" to successors of "slice"
            relinkSuccessorsToNewSource(slice, append, EXEC);
            // remove "slice" node and associated edges
            graph.remove(slice);

            // insert equivalent "preslice" between "append" and each predecessor
            final TableTransform presliceTransform = new TableTransform(Collections.emptyList(), slice.getTransformSpec());
            for (RagEdge edge : new ArrayList<>(append.incomingEdges(EXEC))) {
                final RagNode node = edge.getSource();
                final RagNode preslice = graph.addNode(presliceTransform);
                graph.addEdge(node, preslice, EXEC);
                graph.addEdge(preslice, append, EXEC);
                graph.remove(edge);
            }
            return true;
        }
        return false;
    }



    // --------------------------------------------------------------------
    // mergeSlices()

    boolean mergeSlices() {
        for (final RagNode node : graph.nodes(SLICE)) {
            if (tryMergeSlice(node)) {
                return true;
            }
        }
        return false;
    }

    private boolean tryMergeSlice(final RagNode slice) {
        final List<RagNode> predecessors = slice.predecessors(EXEC);
        if (predecessors.size() == 1) {
            switch(predecessors.get(0).type()) {
                case SOURCE:
                    return mergeSliceToSource(slice);
                case SLICE:
                    mergeSliceToSlice(slice);
                    return true;
                default:
                    return false;
            }
        }
        return false;
    }

    private boolean mergeSliceToSource(final RagNode slice) {
        final RagNode source = slice.predecessor(EXEC);

        // check whether the source supports efficient row range slicing
        final SourceTransformSpec sourceSpec = source.getTransformSpec();
        if ( !sourceSpec.getProperties().supportsRowRange() ) {
            return false;
        }

        // merge indices from predecessor and slice
        final SliceTransformSpec sliceSpec = slice.getTransformSpec();
        final RowRangeSelection sourceRange = sourceSpec.getRowRange();
        final RowRangeSelection sliceRange = sliceSpec.getRowRangeSelection();
        final RowRangeSelection mergedRange = sourceRange.retain(sliceRange);

        // create new merged SOURCE Node
        final SourceTransformSpec mergedSpec = new SourceTransformSpec(sourceSpec.getSourceIdentifier(), sourceSpec.getProperties(), mergedRange);
        final TableTransform mergedTableTransform = new TableTransform(Collections.emptyList(), mergedSpec);
        final RagNode merged = graph.addNode(mergedTableTransform);

        // re-link accesses outputs of source to merged
        merged.setNumColumns(source.numColumns());
        for (AccessId oldId : source.getOutputs()) {
            AccessId newId = merged.getOrCreateOutput(oldId.getColumnIndex());
            for (RagNode consumer : oldId.getConsumers()) {
                if (consumer.replaceInput(oldId, newId)) {
                    newId.addConsumer(consumer);
                }
            }
        }
        // re-link DATA edges of source to merged
        // (link merged to all DATA successors of source)
        relinkSuccessorsToNewSource(source, merged, DATA);

        // link merged to all EXEC successors of slice
        relinkSuccessorsToNewSource(slice, merged, EXEC);

        // remove source and slice (and associated edges)
        graph.remove(source);
        graph.remove(slice);

        return true;
    }

    private void mergeSliceToSlice(final RagNode slice) {
        final RagNode predecessor = slice.predecessor(EXEC);

        // merge indices from predecessor and slice
        final SliceTransformSpec predecessorSpec = predecessor.getTransformSpec();
        final SliceTransformSpec sliceSpec = slice.getTransformSpec();
        final RowRangeSelection predecessorRange = predecessorSpec.getRowRangeSelection();
        final RowRangeSelection sliceRange = sliceSpec.getRowRangeSelection();
        final RowRangeSelection mergedRange = predecessorRange.retain(sliceRange);

        // create new merged SLICE Node
        final TableTransform mergedTableTransform = new TableTransform(Collections.emptyList(), new SliceTransformSpec(mergedRange));
        final RagNode merged = graph.addNode(mergedTableTransform);

        // link all predecessors of predecessor to merged
        relinkPredecessorsToNewTarget(predecessor, merged, EXEC);

        // link merged to all EXEC successors of slice
        relinkSuccessorsToNewSource(slice, merged, EXEC);

        // remove all EXEC edges from slice and predecessor
        final List<RagEdge> edgesToRemove = new ArrayList<>(slice.incomingEdges(EXEC));
        edgesToRemove.addAll(slice.outgoingEdges(EXEC));
        edgesToRemove.addAll(predecessor.incomingEdges(EXEC));
        // this would only be the one outgoing edge to slice, which we have already added:
        // edgesToRemove.addAll(predecessor.outgoingEdges(EXEC));
        for (final RagEdge edge : edgesToRemove) {
            graph.remove(edge);
        }
    }



    // --------------------------------------------------------------------
    // eliminateAppends()

    private boolean eliminateAppends() {
        for (final RagNode node : graph.nodes(APPEND)) {
            if (tryEliminateAppend(node)) {
                return true;
            }
        }
        return false;
    }

    private boolean tryEliminateAppend(final RagNode append) {
        if (getAncestorSources(append).size() == 1) {
            eliminateAppend(append);
            return true;
        } else {
            return false;
        }
    }

    /**
     * Recursively trace EXEC edges backwards from {@code node} to SOURCE nodes.
     * <p>
     * This is used to determine whether an APPEND node can be eliminated. (An APPEND
     * can be eliminated if all EXEC edges link back to a single SOURCE.)
     *
     * @return the set of SOURCE nodes transitively linking to {@code node} via EXEC edges.
     */
    private Set<RagNode> getAncestorSources(final RagNode node) {
        if (node.type() == SOURCE) {
            return Set.of(node);
        }

        final Set<RagNode> sources = new HashSet<>();
        List<RagNode> predecessors = node.predecessors(EXEC);
        for (RagNode predecessor : predecessors) {
            sources.addAll(getAncestorSources(predecessor));
        }
        return sources;
    }

    /**
     * Get the set of effective producers for a given {@code access}.
     * <p>
     * The set of effective producers for an {@code access} is the singleton set
     * containing the producer of the access, except when the producer is a MISSING or
     * a MAP node.
     * <p>
     * Missing values don't really count, because there is nothing to forward or wrap
     * there. So if the producer is a MISSING node, the set of effective producers is
     * empty.
     * <p>
     * We can "skip over" MAP nodes, because they just pass through forward behaviour
     * to their predecessor Node. The set of effective producers of {@code access} is
     * then the union over the effective producers of all the MAP's inputs.
     */
    private Set<RagNode> getEffectiveProducers(final AccessId access) {
        final RagNode producer = access.getProducer();
        if (producer.type() == MISSING) {
            return Collections.emptySet();
        } else if (producer.type() == MAP) {
            final Set<RagNode> producers = new HashSet<>();
            for (AccessId input : producer.getInputs()) {
                producers.addAll(getEffectiveProducers(input));
            }
            return producers;
        } else {
            return Collections.singleton(producer);
        }
    }

    private void eliminateAppend(final RagNode append) {
        // Short-circuit EXEC edges from predecessors to successors
        for (RagNode predecessor : append.predecessors(EXEC)) {
            for (RagNode successor : append.successors(EXEC)) {
                graph.getOrAddEdge(predecessor, successor, EXEC);
            }
        }

        graph.remove(append);

        // For each output in APPEND.outputs[i]:
        //   For each consumer in output.getConsumers():
        //     Find consumer.inputs[j] corresponding to output, and replace by APPEND.inputs[i]
        // Remove APPEND from the consumers of its inputs, and add short-circuited consumers instead.
        // Add new DATA edges from producers to short-circuited consumers.
        final AccessIds inputs = append.getInputs();
        final AccessIds outputs = append.getOutputs();
        for (int i = 0; i < outputs.size(); i++) {
            final AccessId output = outputs.getAtSlot(i);
            final AccessId input = inputs.getAtSlot(i);
            input.removeConsumer(append);
            for (RagNode consumer : output.getConsumers()) {
                if (consumer.replaceInput(output, input)) {
                    input.addConsumer(consumer);
                    if (input.getProducer().type() != MISSING) {
                        graph.getOrAddEdge(input.getProducer(), consumer, DATA);
                    }
                }
            }
        }
    }



    // --------------------------------------------------------------------
    // createExecutionOrderingEdges()

    /**
     * Create ORDER edges by taking the union of DATA and EXEC dependency edges and
     * doing a transitive reduction.
     */
    void createExecutionOrderingEdges() {
        final List<RagNode> vertices = new ArrayList<>(graph.nodes());
        RagGraphUtils.addEdges(graph, vertices, RagGraphUtils.transitiveReduction(
                RagGraphUtils.adjacency(vertices, DATA, EXEC)), ORDER);
    }



    // --------------------------------------------------------------------
    // removeWrapperNodes()

    /**
     * Remove the WRAPPER nodes that represent CONCATENATE inputs. Short-circuit
     * edges going into WRAPPER to the CONCATENATE. (This will add the incoming
     * edges to CONCATENATE in the correct order.)
     */
    void removeWrapperNodes() {
        final ArrayList<RagNode> concatenates = new ArrayList<>(graph.nodes(CONCATENATE));
        for (RagNode concatenate : concatenates) {
            final List<RagNode> wrappers = concatenate.predecessors(ORDER);
            for (int i = 0; i < wrappers.size(); i++) {
                final AccessIds concatenateInputs = concatenate.getInputs(i);
                final RagNode wrapper = wrappers.get(i);
                final AccessIds wrapperInputs = wrapper.getInputs();
                for (int slot = 0; slot < wrapperInputs.size(); slot++) {
                    AccessId id = wrapperInputs.getAtSlot(slot);
                    id.removeConsumer(wrapper);
                    id.addConsumer(concatenate);
                    concatenateInputs.putAtColumnIndex(id, wrapperInputs.columnAtSlot(slot));
                }
                relinkPredecessorsToNewTarget(wrapper, concatenate, EXEC);
                relinkPredecessorsToNewTarget(wrapper, concatenate, DATA);
                relinkPredecessorsToNewTarget(wrapper, concatenate, ORDER);
                graph.remove(wrapper);
            }
        }
    }



    // --------------------------------------------------------------------
    // getFlattenedExecutionOrder()

    /**
     * Returns a topological ordering of the {@code RagGraph}
     * @param policy
     * @return
     */
    List<RagNode> getFlattenedExecutionOrder(final Function<List<RagNode>, RagNode> policy) {
        return RagGraphUtils.topologicalSort(graph, ORDER, List.of(graph.getRoot()), policy);
    }

}
