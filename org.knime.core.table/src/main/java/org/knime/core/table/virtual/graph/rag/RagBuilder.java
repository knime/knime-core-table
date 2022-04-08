package org.knime.core.table.virtual.graph.rag;

import static org.knime.core.table.virtual.graph.rag.RagEdgeType.DATA;
import static org.knime.core.table.virtual.graph.rag.RagEdgeType.EXEC;
import static org.knime.core.table.virtual.graph.rag.RagEdgeType.ORDER;
import static org.knime.core.table.virtual.graph.rag.RagEdgeType.SPEC;
import static org.knime.core.table.virtual.graph.rag.RagGraphUtils.topologicalSort;
import static org.knime.core.table.virtual.graph.rag.RagNodeType.APPEND;
import static org.knime.core.table.virtual.graph.rag.RagNodeType.CONCATENATE;
import static org.knime.core.table.virtual.graph.rag.RagNodeType.MAP;
import static org.knime.core.table.virtual.graph.rag.RagNodeType.MISSING;
import static org.knime.core.table.virtual.graph.rag.RagNodeType.ROWFILTER;
import static org.knime.core.table.virtual.graph.rag.RagNodeType.SLICE;
import static org.knime.core.table.virtual.graph.rag.RagNodeType.SOURCE;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.knime.core.table.cursor.Cursors;
import org.knime.core.table.row.Selection.RowRangeSelection;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.DefaultColumnarSchema;
import org.knime.core.table.schema.traits.DataTraits;
import org.knime.core.table.schema.traits.DefaultDataTraits;
import org.knime.core.table.virtual.TableTransform;
import org.knime.core.table.virtual.VirtualTable;
import org.knime.core.table.virtual.graph.cap.CapBuilder;
import org.knime.core.table.virtual.spec.AppendMissingValuesTransformSpec;
import org.knime.core.table.virtual.spec.ColumnFilterTransformSpec;
import org.knime.core.table.virtual.spec.MapTransformSpec;
import org.knime.core.table.virtual.spec.PermuteTransformSpec;
import org.knime.core.table.virtual.spec.RowFilterTransformSpec;
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
        nodes.sort(Comparator.comparingInt(node -> {
            switch (node.type()) {
                case SOURCE:
                default:
                    return 0;
                case SLICE:
                    return 1;
                case ROWFILTER:
                    return 2;
                case APPEND:
                    return 3;
                case CONCATENATE:
                    return 4;
                case MAP:
                    return 5;
            }
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
        if (node.type() != RagNodeType.CONSUMER) {
            throw new IllegalArgumentException();
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
        RagNode producer = accessId.getProducer();
        switch (producer.type()) {
            case SOURCE: {
                final SourceTransformSpec spec = producer.getTransformSpec();
                return spec.getSchema().getSpec(accessId.getColumnIndex());
            }
            case MISSING: {
                final MissingValuesSourceTransformSpec spec = producer.getTransformSpec();
                return spec.getMissingValueSpecs().get(accessId.getColumnIndex());
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
                // FIXME missing column might also have traits (e.g. LogicalTypeTrait in KNIME)
                return DefaultDataTraits.EMPTY;
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
     * no row-filter (or other nodes that would destroy lookahead capability.)
     *
     * @param orderedRag a linearized {@code RagGraph}
     * @return {@code true} if the {@code orderedRag} supports {@code LookaheadCursor}s
     */
    public static boolean supportsLookahead(final List<RagNode> orderedRag) {
        final RagNode node = orderedRag.get(orderedRag.size() - 1);
        if (node.type() != RagNodeType.CONSUMER) {
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
            case CONSUMER: {
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
            case COLPERMUTE:
            case APPENDMISSING:
                throw new IllegalArgumentException(
                        "Unexpected RagNode type " + node.type() + ".");
            default:
                throw new IllegalStateException("Unexpected value: " + node.type());
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
    private static final EnumSet<RagNodeType> executableNodeTypes = EnumSet.of(APPEND, SOURCE, CONCATENATE, SLICE, ROWFILTER);

    /**
     * Spawning nodes create new sub-trees of their SPEC predecessor nodes. This
     * prevents (incorrect) fork-join type fusion of branches when forwarding structure
     * diverges.
     */
    private static final EnumSet<RagNodeType> spawningNodeTypes = EnumSet.of(SLICE);

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

        final var consumerTransform = new TableTransform(//
                List.of(tableTransform),//
                new ConsumerTransformSpec());
        final RagNode root = createNodes(consumerTransform, new HashMap<>());
        graph.setRoot(root);

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
                graph.addEdge(input, node, SPEC);
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
                case SLICE:
                case ROWFILTER:
                case IDENTITY:
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
                    numColumns = ((ColumnFilterTransformSpec)spec).getColumnSelection().length;
                    break;
                case COLPERMUTE:
                    numColumns = ((PermuteTransformSpec)spec).getPermutation().length;
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
                    for (int j = 0; j < numInputColumns; j++) {
                        traceAndLinkAccess(j, node);
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
            case CONSUMER: {
                accessIds[0] = traceAccess(i, node.predecessor(SPEC));
                break;
            }
            case SOURCE:
            case SLICE:
            case APPENDMISSING:
            case COLFILTER:
            case COLPERMUTE:
                throw new IllegalArgumentException();
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
                    // get/add the index of the missing AccessId for that DataSpec
                    // and get/add missingValuesSource output AccessId for that index
                    final RagNode missingValuesSource = graph.getMissingValuesSource();
                    return missingValuesSource.getOrCreateOutput(missingValueColumns.getOrAdd(dataSpec));
                }
            case COLFILTER:
                final int[] selection = ((ColumnFilterTransformSpec)spec).getColumnSelection();
                return traceAccess(selection[i], node.predecessor(SPEC));
            case COLPERMUTE:
                final int[] permutation = ((PermuteTransformSpec)spec).getPermutation();
                return traceAccess(permutation[i], node.predecessor(SPEC));
            case CONSUMER:
            case MISSING:
            default:
                throw new IllegalArgumentException();
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
            case COLPERMUTE:
            case MISSING:
            case APPENDMISSING:
            case MAP:
            case IDENTITY:
                // do nothing
                break;
            case SLICE:
            case CONCATENATE:
            case CONSUMER:
            case APPEND:
            case ROWFILTER:
                traceExecConsumer(node, node);
                break;
            default:
                throw new IllegalArgumentException();
        }
    }

    /**
     * Recursively trace along reverse SPEC edges to insert EXEC edges between
     * executable nodes. A node is executable if it can be forwarded. Executable nodes
     * depend on each other for executing forward steps in order to determine that
     * everybody is on the same row (without looking at produced/consumed data).
     * <p>
     *
     * @param dest    the node from which the current recursive trace started. All
     *                potentially created EXEC edges link <em>to</em> the {@code dest} node.
     * @param current the node which is currently visited along the recursive trace. An
     *                EXEC edge linking <em>from</em> the {@code current} node is created, if the
     *                {@code current} node is an executable node.
     */
    // TODO: rename??? Why is it called "Consumer"???
    private void traceExecConsumer(final RagNode dest, final RagNode current) {

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
                    if (dest.type() != ROWFILTER) {
                        graph.getOrAddEdge(current, dest, EXEC);
                    }
                    // continue tracing recursively
                } else {
                    graph.getOrAddEdge(current, dest, EXEC);
                    return; // no further recursion
                }
            }
        }
        // recurse along reverse SPEC edges
        for (RagNode predecessor : current.predecessors(SPEC)) {
            traceExecConsumer(dest, predecessor);
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
            // TODO other optimizations
        }
    }



    // --------------------------------------------------------------------
    // moveSliceBeforeAppend()

    boolean moveSlicesBeforeAppends() {
        for (final RagNode node : graph.nodes()) {
            if (node.type() == SLICE && tryMoveSliceBeforeAppend(node)) {
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
            for (RagNode node : slice.successors(EXEC)) {
                graph.getOrAddEdge(append, node, EXEC);
            }
            // remove "slice" node and associated edges
            graph.remove(slice);

            // insert equivalent "preslice" between "append" and each predecessor
            final TableTransform presliceTransform = new TableTransform(Collections.emptyList(), slice.getTransformSpec());
            for (RagNode node : append.predecessors(EXEC)) {
                final RagNode preslice = graph.addNode(presliceTransform);
                graph.addEdge(node, preslice, EXEC);
                graph.addEdge(preslice, append, EXEC);
            }
            return true;
        }
        return false;
    }



    // --------------------------------------------------------------------
    // mergeSlices()

    boolean mergeSlices() {
        for (final RagNode node : graph.nodes()) {
            if (node.type() == SLICE && tryMergeSlice(node)) {
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
        for (RagEdge edge : source.outgoingEdges(DATA)) {
            graph.addEdge(merged, edge.getTarget(), DATA);
        }

        // link merged to all EXEC successors of slice
        for (RagNode node : slice.successors(EXEC)) {
            graph.getOrAddEdge(merged, node, EXEC);
        }

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
        for (RagNode node : predecessor.predecessors(EXEC)) {
            graph.getOrAddEdge(node, merged, EXEC);
        }

        // link merged to all successors of slice
        for (RagNode node : slice.successors(EXEC)) {
            graph.getOrAddEdge(merged, node, EXEC);
        }

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
        for (final RagNode node : graph.nodes()) {
            if (node.type() == APPEND && tryEliminateAppend(node)) {
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
        if (node.type() == SOURCE)
            return Set.of(node);

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
    // TODO: does this belong here?

    /**
     * Returns a topological ordering of the {@code RagGraph}
     * @param policy
     * @return
     */
    List<RagNode> getFlattenedExecutionOrder(final Function<List<RagNode>, RagNode> policy) {
        return RagGraphUtils.topologicalSort(graph, ORDER, List.of(graph.getRoot()), policy);
    }

}
