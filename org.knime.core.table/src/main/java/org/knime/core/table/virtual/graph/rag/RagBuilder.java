/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 */
package org.knime.core.table.virtual.graph.rag;

import static org.knime.core.table.virtual.graph.rag.RagEdgeType.DATA;
import static org.knime.core.table.virtual.graph.rag.RagEdgeType.EXEC;
import static org.knime.core.table.virtual.graph.rag.RagEdgeType.ORDER;
import static org.knime.core.table.virtual.graph.rag.RagEdgeType.SPEC;
import static org.knime.core.table.virtual.graph.rag.RagNodeType.APPEND;
import static org.knime.core.table.virtual.graph.rag.RagNodeType.CONCATENATE;
import static org.knime.core.table.virtual.graph.rag.RagNodeType.CONSUMER;
import static org.knime.core.table.virtual.graph.rag.RagNodeType.MAP;
import static org.knime.core.table.virtual.graph.rag.RagNodeType.MATERIALIZE;
import static org.knime.core.table.virtual.graph.rag.RagNodeType.MISSING;
import static org.knime.core.table.virtual.graph.rag.RagNodeType.OBSERVER;
import static org.knime.core.table.virtual.graph.rag.RagNodeType.ROWFILTER;
import static org.knime.core.table.virtual.graph.rag.RagNodeType.ROWINDEX;
import static org.knime.core.table.virtual.graph.rag.RagNodeType.SLICE;
import static org.knime.core.table.virtual.graph.rag.RagNodeType.SOURCE;
import static org.knime.core.table.virtual.graph.rag.RagNodeType.WRAPPER;

import java.util.ArrayList;
import java.util.Arrays;
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

import org.knime.core.table.row.Selection.RowRangeSelection;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpecs;
import org.knime.core.table.schema.DataSpecs.DataSpecWithTraits;
import org.knime.core.table.virtual.TableTransform;
import org.knime.core.table.virtual.graph.cap.CapBuilder;
import org.knime.core.table.virtual.graph.debug.VirtualTableDebugging;
import org.knime.core.table.virtual.graph.rag.RagNode.AccessValidity;
import org.knime.core.table.virtual.spec.AppendMissingValuesTransformSpec;
import org.knime.core.table.virtual.spec.MapTransformSpec;
import org.knime.core.table.virtual.spec.MaterializeTransformSpec;
import org.knime.core.table.virtual.spec.ObserverTransformSpec;
import org.knime.core.table.virtual.spec.RowFilterTransformSpec;
import org.knime.core.table.virtual.spec.RowIndexTransformSpec;
import org.knime.core.table.virtual.spec.SelectColumnsTransformSpec;
import org.knime.core.table.virtual.spec.SliceTransformSpec;
import org.knime.core.table.virtual.spec.SourceTransformSpec;
import org.knime.core.table.virtual.spec.TableTransformSpec;

/**
 * Create a {@code RagGraph} from a {@code VirtualTable} spec.
 *
 * <ol>
 * <li>{@link SpecGraphBuilder#buildSpecGraph(TableTransform)}:
 *      Build RagGraph nodes for each TableTransform in the given VirtualTable, and SPEC
 *      edges between them. Set numColumns for each node.</li>
 * <li>{@link #traceExec()}:
 *      Recursively trace along reverse SPEC edges to insert EXEC edges between
 *      executable nodes, starting in turn from each node of the graph.</li>
 * <li>{@link #traceAccesses()}:
 *      Trace each access from its consumer to its producer, starting with accesses
 *      consumed by the root node, and walking along SPEC edges.</li>
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

    /**
     * Optimize and sequentialize the given {@code specGraph} using the default
     * sequentialization policy.
     * <p>
     * The {@code specGraph} is not modified, optimization is done on a copy.
     *
     * @param specGraph
     *          the spec graph (see {@link SpecGraphBuilder})
     */
    public static List<RagNode> createOrderedRag(final RagGraph specGraph) {
        return createOrderedRag(specGraph, DEFAULT_POLICY, true);
    }

    /**
     * Optimize and sequentialize the given {@code specGraph} using the default
     * sequentialization policy.
     * <p>
     * If {@code copySpecGraph==true} then a copy of {@code specGraph} will be
     * used. If {@code copySpecGraph==false} then {@code specGraph} will be
     * modified in place. Note that in-place modification destroys the
     * "spec-graph-ness", because SPEC edges are removed in the process.
     *
     * @param specGraph
     *          the spec graph (see {@link SpecGraphBuilder})
     * @param copySpecGraph {@code true}, if optimization should work on a
     *          separate copy of {@code specGraph}.
     * @return sequentialized {@code RagGraph} that can be passed to {@link
     *          CapBuilder#createCursorAssemblyPlan}.
     */
    public static List<RagNode> createOrderedRag(
            final RagGraph specGraph,
            final boolean copySpecGraph) {
        return createOrderedRag(specGraph, DEFAULT_POLICY, copySpecGraph);
    }

    /**
     * Optimize and sequentialize the given {@code specGraph}.
     * <p>
     * If {@code copySpecGraph==true} then a copy of {@code specGraph} will be
     * used. If {@code copySpecGraph==false} then {@code specGraph} will be
     * modified in place. Note that in-place modification destroys the
     * "spec-graph-ness", because SPEC edges are removed in the process.
     * <p>
     * If there are choices in how the graph may be sequentialized, the given
     * {@code policy} is used: Given a list of possible {@code RagNode}s, the
     * policy should pick the one that should come next in the sequence. (The
     * default policy tries to order nodes to avoid unnecessary work. For
     * example, when choosing between a SLICE and a MAP, it would pick SLICE,
     * because that avoids to compute the MAP function on rows that would be
     * sliced away later.)
     *
     * @param specGraph
     *          the spec graph (see {@link SpecGraphBuilder})
     * @param policy sequentialization policy
     * @param copySpecGraph {@code true}, if optimization should work on a
     *          separate copy of {@code specGraph}
     * @return sequentialized {@code RagGraph} that can be passed to {@link
     *          CapBuilder#createCursorAssemblyPlan}.
     */
    public static List<RagNode> createOrderedRag(
            final RagGraph specGraph,
            final Function<List<RagNode>, RagNode> policy,
            final boolean copySpecGraph) {

        try( var logger = VirtualTableDebugging.createLogger() ) {

            final RagBuilder specs = new RagBuilder(copySpecGraph ? specGraph.copy() : specGraph);
            logger.appendRagGraph("buildSpecGraph()", "SPEC edges", specs.graph);

            specs.traceExec();
            logger.appendRagGraph("traceExec()", "adds EXEC edges", specs.graph);

            specs.traceAccesses();
            logger.appendRagGraph("traceAccesses()", "adds DATA edges", specs.graph);

            specs.optimize( logger );

            specs.createExecutionOrderingEdges();
            logger.appendRagGraph("after createExecutionOrderingEdges()", "adds ORDER edges", specs.graph);

            specs.removeWrapperNodes();
            logger.appendRagGraph("after removeWrapperNodes()", "short-circuit wrappers before concatenate",
                    specs.graph);

            final List<RagNode> order = specs.getFlattenedExecutionOrder(policy);
            // If we need missing value columns, prepend the MISSING source to the execution order
            // (it should not be connected by ORDER edges)
            // TODO: Avoid this special case logic?
            final RagNode missingValuesSource = specs.graph.getMissingValuesSource();
            if (!missingValuesSource.getOutputs().isEmpty()) {
                order.add(0, missingValuesSource);
            }
            logger.appendOrderedRagGraph("flattened execution order", "adds FLATTENED_ORDER edges (just for visualization)", specs.graph, order);

            return order;
        }
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

        final AccessIds inputs = node.getInputs();
        final DataSpecWithTraits[] specs = new DataSpecWithTraits[inputs.size()];
        Arrays.setAll(specs, i -> getSpecWithTraits(inputs.getAtSlot(i)));
        return ColumnarSchema.of(specs);
    }

    private static DataSpecWithTraits getSpecWithTraits(final AccessId accessId) {
        final RagNode producer = accessId.getProducer();
        return switch (producer.type()) {
            case SOURCE -> {
                final SourceTransformSpec spec = producer.getTransformSpec();
                yield spec.getSchema().getSpecWithTraits(accessId.getColumnIndex());
            }
            case MISSING -> {
                final MissingValuesSourceTransformSpec spec = producer.getTransformSpec();
                yield spec.getMissingValueSpecs().get(accessId.getColumnIndex());
            }
            case APPEND, CONCATENATE -> {
                final int slot = producer.getOutputs().slotIndexOf(accessId);
                yield getSpecWithTraits(producer.getInputs(0).getAtSlot(slot));
            }
            case MAP -> {
                final MapTransformSpec spec = producer.getTransformSpec();
                yield spec.getMapperFactory().getOutputSchema().getSpecWithTraits(accessId.getColumnIndex());
            }
            case ROWINDEX -> DataSpecs.LONG;
            case SLICE, APPENDMISSING, COLFILTER, ROWFILTER, CONSUMER, MATERIALIZE, WRAPPER, IDENTITY, OBSERVER ->
                    throw new IllegalArgumentException("unexpected node type " + producer.type());
        };
    }

    final RagGraph graph;

    /**
     * Executable nodes are linked by EXEC edges.
     * <p>
     * A node is "executable" if it can be forwarded. Executable nodes
     * depend on each other for executing forward steps in order to determine that
     * everybody is on the same row (without looking at produced/consumed data).
     */
    // TODO: move to RagNodeType
    public static final EnumSet<RagNodeType> executableNodeTypes = EnumSet.of(APPEND, SOURCE, CONCATENATE, SLICE, ROWFILTER, WRAPPER, ROWINDEX, OBSERVER);

    /**
     * Sink nodes are consumer-like endpoints.
     */
    public static final EnumSet<RagNodeType> sinkNodeTypes = EnumSet.of(CONSUMER, MATERIALIZE);

    RagBuilder() {
        graph = new RagGraph();
    }

    RagBuilder(final RagGraph graph) {
        this.graph = graph;
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

        graph.nodes(OBSERVER).forEach(node -> {
            final int numInputColumns = node.<ObserverTransformSpec>getTransformSpec().getColumnSelection().length;
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
            case MAP:
            case OBSERVER: {
                final int[] selection = getColumnSelection(node.getTransformSpec());
                final int j = ( i == selection.length ) // is i the added row index column?
                        ? node.predecessor(SPEC).numColumns() - 1 // last predecessor column is row index
                        : selection[i];
                accessIds[0] = traceAccess(j, node.predecessor(SPEC));
                break;
            }
            case ROWFILTER: {
                final int[] selection = node.<RowFilterTransformSpec>getTransformSpec().getColumnSelection();
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
            case MISSING:
            case IDENTITY:
            case ROWINDEX:
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

    private int[] getColumnSelection(TableTransformSpec spec) {
        if (spec instanceof MapTransformSpec m)
            return m.getColumnSelection();
        else if (spec instanceof ObserverTransformSpec o)
            return o.getColumnSelection();
        else
            throw new IllegalArgumentException();
    }

    /**
     * Recursively trace the access <em><b>output</b></em> by {@code node} at column
     * {@code i} to its producer.
     */
    private AccessId traceAccess(final int i, final RagNode node) {
        switch (node.type()) {
            case SOURCE:
                return node.getOrCreateOutput(i);
            case MAP:
                // N.B. This would be repeated as we visit each outgoing column i. However, we only
                // need to trace the map input columns once. Therefore, after the input-tracing has
                // happened the node is marked, and the tracing is not repeated.
                if (node.getMark() == 0) {
                    node.setMark(1);
                    final MapTransformSpec spec = node.getTransformSpec();
                    final int numInputColumns = spec.getColumnSelection().length;
                    for (int j = 0; j < numInputColumns; j++) {
                        traceAndLinkAccess(j, node);
                    }
                }
                return node.getOrCreateOutput(i);
            case ROWINDEX: {
                final int numPredecessorColumns = node.predecessor(SPEC).numColumns();
                if (i < numPredecessorColumns) {
                    return traceAccess(i, node.predecessor(SPEC));
                } else {
                    return node.getOrCreateOutput(i);
                }
            }
            case ROWFILTER:
            case SLICE:
            case IDENTITY:
            case OBSERVER:
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
                    final AppendMissingValuesTransformSpec spec = node.getTransformSpec();
                    final ColumnarSchema appendedSchema = spec.getAppendedSchema();
                    final DataSpecWithTraits dataSpec = appendedSchema.getSpecWithTraits(i - numPredecessorColumns);
                    // add missingValuesSource output AccessId for dataSpec
                    return graph.getMissingValuesAccessId(dataSpec, node.validity());
                }
            case COLFILTER:
                final SelectColumnsTransformSpec spec = node.getTransformSpec();
                final int[] selection = spec.getColumnSelection();
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
            case ROWINDEX:
            case OBSERVER:
            case MAP:
                traceExecConsumer(node, node, false);
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
        optimize(new VirtualTableDebugging.NullLogger());
    }

    void optimize(VirtualTableDebugging.Logger logger) {
        graph.trim();
        graph.transitiveReduction(EXEC);
        logger.appendRagGraph("optimize()", "trim unused nodes and edges", graph);
        boolean changed = true;
        while (changed) {
            changed = false;
            if ( pushMapExecs() ) {
                logger.appendRagGraph("pushMapExecs", "(optimize step)", graph);
                changed = true;
            }
            if ( eliminateRowIndexes() ) {
                logger.appendRagGraph("eliminateRowIndexes", "(optimize step)", graph);
                changed = true;
            }
            if ( eliminateAppends() ) {
                logger.appendRagGraph("eliminateAppends", "(optimize step)", graph);
                changed = true;
            }
            if ( mergeSlices() ) {
                logger.appendRagGraph("mergeSlices", "(optimize step)", graph);
                changed = true;
            }
            if ( mergeRowIndexSiblings() ) {
                logger.appendRagGraph("mergeRowIndexSiblings", "(optimize step)", graph);
                changed = true;
            }
            if ( mergeRowIndexSequence() ) {
                logger.appendRagGraph("mergeRowIndexSequence", "(optimize step)", graph);
                changed = true;
            }
            if ( moveSlicesBeforeObserves() ) {
                logger.appendRagGraph("moveSlicesBeforeObserves", "(optimize step)", graph);
                changed = true;
            }
            if ( moveSlicesBeforeAppends() ) {
                logger.appendRagGraph("moveSlicesBeforeAppends", "(optimize step)", graph);
                changed = true;
            }
            if ( moveSlicesBeforeRowIndexes() ) {
                logger.appendRagGraph("moveSlicesBeforeRowIndexes", "(optimize step)", graph);
                changed = true;
            }
            if ( moveSlicesBeforeConcatenates() ) {
                logger.appendRagGraph("moveSlicesBeforeConcatenates", "(optimize step)", graph);
                changed = true;
            }
            if ( eliminateSingletonConcatenates() ) {
                logger.appendRagGraph("eliminateSingletonConcatenates", "(optimize step)", graph);
                changed = true;
            }
            // TODO other optimizations
        }
    }



    // --------------------------------------------------------------------
    // pushMapExecs()

    boolean pushMapExecs() {
        boolean changed = false;
        for (final RagNode node : graph.nodes(MAP)) {
            if (tryPushMapExec(node)) {
                changed = true;
            }
        }
        return changed;
    }

    private boolean tryPushMapExec(final RagNode map) {
        boolean changed = false;
        var incoming = new ArrayList<>(map.incomingEdges(EXEC));
        for (RagEdge edge : incoming) {
            final RagNode predecessor = edge.getSource();
            if ( predecessor.validity().getProducer() != predecessor ) {
                graph.replaceEdgeSource(edge, predecessor.validity().getProducer());
                changed = true;
            }
        }
        return changed;
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

            concatenate.validity().replaceInConsumersWith(wrapper.validity());

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
                if (input.getProducer().type() != MISSING) {
                    for (RagNode consumer : output.getConsumers()) {
                        graph.getOrAddEdge(input.getProducer(), consumer, DATA);
                    }
                }
                output.replaceInConsumersWith(input);
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
                final long r = RagGraphProperties.numRows(node);
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
                    graph.relinkPredecessorsToNewTarget(wrapper, preslice, EXEC);
                    graph.addEdge(preslice, wrapper, EXEC);
                }
                // Note, that if rows.allSelected() == true then we don't have
                // to do anything. wrapper just stays connected to CONCATENATE.
            }

            // short-circuit SLICE successors to CONCATENATE
            graph.relinkSuccessorsToNewSource(slice, concatenate, EXEC);

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
    // moveSliceBeforeObserves()

    boolean moveSlicesBeforeObserves() {
        for (final RagNode node : graph.nodes(SLICE)) {
            if (tryMoveSliceBeforeObserve(node)) {
                return true;
            }
        }
        return false;
    }

    private boolean tryMoveSliceBeforeObserve(final RagNode slice) {
        final List<RagNode> predecessors = slice.predecessors(EXEC);
        if (predecessors.size() == 1 && predecessors.get(0).type() == OBSERVER) {
            final RagNode observer = predecessors.get(0);

            // remove "observer" --> "slice" link
            graph.remove(slice.incomingEdges(EXEC).iterator().next());

            // re-link successors of "slice" to "observer"
            graph.relinkSuccessorsToNewSource(slice, observer, EXEC);

            // re-link predecessors of "observer" to "slice"
            graph.relinkPredecessorsToNewTarget(observer, slice, EXEC);

            // add "slice" --> "observer" link
            graph.addEdge(slice, observer, EXEC);

            return true;
        }
        return false;
    }



    // --------------------------------------------------------------------
    // moveSliceBeforeRowIndex()

    boolean moveSlicesBeforeRowIndexes() {
        for (final RagNode node : graph.nodes(SLICE)) {
            if (tryMoveSliceBeforeRowIndex(node)) {
                return true;
            }
        }
        return false;
    }

    private boolean tryMoveSliceBeforeRowIndex(final RagNode slice) {
        final List<RagNode> predecessors = slice.predecessors(EXEC);
        if (predecessors.size() == 1 && predecessors.get(0).type() == ROWINDEX) {
            final RagNode rowIndex = predecessors.get(0);

            // create offsetRowindex node
            // (add offset to rowindex to compensate for slice)
            final SliceTransformSpec sliceSpec = slice.getTransformSpec();
            final RowIndexTransformSpec rowIndexSpec = rowIndex.getTransformSpec();
            final long offset = rowIndexSpec.getOffset() + Math.max(sliceSpec.getRowRangeSelection().fromIndex(), 0);
            final RowIndexTransformSpec offsetSpec = new RowIndexTransformSpec(offset);
            final TableTransform offsetTransform = new TableTransform(Collections.emptyList(), offsetSpec);
            final RagNode offsetRowIndex = graph.addNode(offsetTransform);
            offsetRowIndex.setNumColumns(rowIndex.numColumns());

            // insert "offsetRowindex" between "slice" and its successors:
            graph.relinkSuccessorsToNewSource(slice, offsetRowIndex, EXEC);
            graph.addEdge(slice, offsetRowIndex, EXEC);

            // remove "rowindex" and link predecessors directly to "slice"
            graph.relinkPredecessorsToNewTarget(rowIndex, slice, EXEC);
            graph.remove(rowIndex);

            // For the single output oldOutput in rowIndex.getOutputs():
            //   For each consumer in oldOutput.getConsumers():
            //     Find consumer.inputs[j] corresponding to oldOutput, and replace by newOutput
            // Add new DATA edges from offsetRowIndex to the (previously) consumers of rowIndex.
            final AccessId oldOutput = rowIndex.getOutputs().getAtSlot(0);
            final AccessId newOutput = offsetRowIndex.getOrCreateOutput(oldOutput.getColumnIndex());
            for (RagNode consumer : oldOutput.getConsumers()) {
                graph.addEdge(offsetRowIndex, consumer, DATA);
            }
            oldOutput.replaceInConsumersWith(newOutput);

            return true;
        }
        return false;
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
            graph.relinkSuccessorsToNewSource(slice, append, EXEC);
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
    // mergeRowIndexSequence()

    boolean mergeRowIndexSequence() {
        for (final RagNode node : graph.nodes(ROWINDEX)) {
            if (tryMergeRowIndexSequence(node)) {
                return true;
            }
        }
        return false;
    }

    private boolean tryMergeRowIndexSequence(final RagNode rowIndex) {
        if (rowIndex.getOutputs().isEmpty())
            return false;
        final List<RagNode> successors = rowIndex.successors(EXEC);
        if (successors.size() == 1) {
            final RagNode successor = successors.get(0);
            if ( successor.type() == ROWINDEX) {
                // successor is another ROWINDEX
                final RagNode rowIndex2 = successor;
                if (rowIndex2.getOutputs().isEmpty())
                    return false;
                final RowIndexTransformSpec spec = rowIndex.getTransformSpec();
                final RowIndexTransformSpec spec2 = rowIndex2.getTransformSpec();
                if (spec.getOffset() == spec2.getOffset()) {
                    // merge them
                    mergeRowIndexes(rowIndex, rowIndex2);
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Merge {@code rowIndex2} into {@code rowIndex}, removing {@code rowIndex2}.
     */
    private void mergeRowIndexes(final RagNode rowIndex, final RagNode rowIndex2) {
        // attach all successors of rowIndex2 to rowIndex (both EXEC and DATA)
        graph.relinkSuccessorsToNewSource(rowIndex2, rowIndex, EXEC, DATA);

        // For the single output in rowIndex2.getOutputs():
        //   For each consumer in output.getConsumers():
        //     Find consumer.inputs[j] corresponding to output, and replace by single output in rowIndex.getOutputs()
        final AccessId output2 = rowIndex2.getOutputs().getAtSlot(0);
        final AccessId output = rowIndex.getOutputs().getAtSlot(0);
        output2.replaceInConsumersWith(output);

        // and remove rowIndex2
        graph.remove(rowIndex2);
    }



    // --------------------------------------------------------------------
    // mergeRowIndexSiblings()

    boolean mergeRowIndexSiblings() {
        for (final RagNode node : graph.nodes(ROWINDEX)) {
            if (tryMergeRowIndexSiblings(node)) {
                return true;
            }
        }
        return false;
    }

    private boolean tryMergeRowIndexSiblings(final RagNode rowIndex) {
        if (rowIndex.getOutputs().isEmpty())
            return false;
        final List<RagNode> predecessors = rowIndex.predecessors(EXEC);
        if (predecessors.size() == 1) {
            final RagNode node = predecessors.get(0);
            final RowIndexTransformSpec spec = rowIndex.getTransformSpec();
            for (RagNode successor : node.successors(EXEC)) {
                if ( successor.type() == ROWINDEX && !successor.equals(rowIndex)) {
                    // successor is another ROWINDEX
                    final RagNode rowIndex2 = successor;
                    if (rowIndex2.getOutputs().isEmpty())
                        return false;
                    final RowIndexTransformSpec spec2 = rowIndex2.getTransformSpec();
                    if (spec.getOffset() == spec2.getOffset()) {
                        // merge them
                        mergeRowIndexes(rowIndex, rowIndex2);
                        return true;
                    }
                }
            }
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
            oldId.replaceInConsumersWith(newId);
            oldId.getValidity().replaceInConsumersWith(newId.getValidity());
        }
        // re-link DATA edges of source to merged
        // (link merged to all DATA successors of source)
        graph.relinkSuccessorsToNewSource(source, merged, DATA);

        // link merged to all EXEC successors of slice
        graph.relinkSuccessorsToNewSource(slice, merged, EXEC);

        // remove slice (and associated edges)
        graph.remove(slice);

        // re-link EXEC edges of source to merged
        // (link merged to all EXEC successors of source -- except slice, which has been removed already)
        graph.relinkSuccessorsToNewSource(source, merged, EXEC);

        // remove source (and associated edges)
        graph.remove(source);

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

        // link merged to all EXEC successors of slice
        graph.relinkSuccessorsToNewSource(slice, merged, EXEC);

        // remove slice (and associated edges)
        graph.remove(slice);

        // link all successors of predecessors to merged
        // (except slice, which has been removed already)
        graph.relinkSuccessorsToNewSource(predecessor, merged, EXEC);

        // link all predecessors of predecessor to merged
        graph.relinkPredecessorsToNewTarget(predecessor, merged, EXEC);

        // remove source (and associated edges)
        graph.remove(predecessor);
    }



    // --------------------------------------------------------------------
    // eliminateRowIndexes()

    private boolean eliminateRowIndexes() {
        for (final RagNode node : graph.nodes(ROWINDEX)) {
            if (tryEliminateRowIndex(node)) {
                return true;
            }
        }
        return false;
    }

    private boolean tryEliminateRowIndex(final RagNode node) {
        // if the rowindex value is never used, remove the node
        if (node.getOutputs().isEmpty()) {
            // Short-circuit EXEC edges from predecessors to successors
            for (RagNode predecessor : node.predecessors(EXEC)) {
                for (RagNode successor : node.successors(EXEC)) {
                    graph.getOrAddEdge(predecessor, successor, EXEC);
                }
            }

            graph.remove(node);
            return true;
        } else {
            return false;
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
        if (getAncestorValidities(append).size() == 1 && getAncestorSources(append).size() == 1) {
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

    private Set<AccessValidity> getAncestorValidities(final RagNode node) {
        Set<AccessValidity> validities = new HashSet<>();
        List<RagNode> predecessors = node.predecessors(EXEC);
        for (RagNode predecessor : predecessors) {
            validities.add(predecessor.validity());
        }
        return validities;
    }

    private void eliminateAppend(final RagNode append) {
        // Short-circuit EXEC edges from predecessors to successors
        for (RagNode predecessor : append.predecessors(EXEC)) {
            for (RagNode successor : append.successors(EXEC)) {
                graph.getOrAddEdge(predecessor, successor, EXEC);
            }
        }

        final AccessValidity predecessorValidity = append.predecessors(EXEC).get(0).validity();
        append.validity().replaceInConsumersWith(predecessorValidity);

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
            if (input.getProducer().type() != MISSING) {
                for (RagNode consumer : output.getConsumers()) {
                    graph.getOrAddEdge(input.getProducer(), consumer, DATA);
                }
            }
            input.removeConsumer(append);
            output.replaceInConsumersWith(input);
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
                graph.relinkPredecessorsToNewTarget(wrapper, concatenate, EXEC, DATA, ORDER);
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
