package org.knime.core.table.virtual.graph.rag3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.knime.core.table.row.Selection;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.graph.cap.CapAccessId;
import org.knime.core.table.virtual.graph.cap.CapNode;
import org.knime.core.table.virtual.graph.cap.CapNodeAppend;
import org.knime.core.table.virtual.graph.cap.CapNodeConcatenate;
import org.knime.core.table.virtual.graph.cap.CapNodeConsumer;
import org.knime.core.table.virtual.graph.cap.CapNodeMap;
import org.knime.core.table.virtual.graph.cap.CapNodeRowFilter;
import org.knime.core.table.virtual.graph.cap.CapNodeRowIndex;
import org.knime.core.table.virtual.graph.cap.CapNodeSlice;
import org.knime.core.table.virtual.graph.cap.CapNodeSource;
import org.knime.core.table.virtual.graph.cap.CursorAssemblyPlan;
import org.knime.core.table.virtual.spec.MapTransformSpec;
import org.knime.core.table.virtual.spec.RowFilterTransformSpec;
import org.knime.core.table.virtual.spec.RowIndexTransformSpec;
import org.knime.core.table.virtual.spec.SliceTransformSpec;
import org.knime.core.table.virtual.spec.SourceTableProperties;
import org.knime.core.table.virtual.spec.SourceTransformSpec;

class BuildCap {

    private final List<CapNode> cap;

    private final Map<TableTransformGraph.Node, CapNode> capNodes;

    private final Map<AccessId, CapAccessId> capAccessIds;

    private final Map<UUID, ColumnarSchema> sourceSchemas;

    BuildCap(final BranchGraph sequentializedGraph) {
        cap = new ArrayList<>();
        capNodes = new HashMap<>();
        capAccessIds = new HashMap<>();
        sourceSchemas = new HashMap<>();

        BranchGraph.BranchEdge branch = sequentializedGraph.rootBranch;

        final CapNode capNode = appendBranch(branch);
        final CapAccessId[] inputs = capAccessIdsFor(sequentializedGraph.tableTransformGraph.terminal().accesses());
        cap.add(new CapNodeConsumer(index++, inputs, capNode.index()));
    }

    public static CursorAssemblyPlan getCursorAssemblyPlan(final BranchGraph sequentializedGraph) {
        final BuildCap builder = new BuildCap(sequentializedGraph);
        final SourceTableProperties.CursorType cursorType =
                sequentializedGraph.tableTransformGraph.supportedCursorType();
        System.out.println("cursorType = " + cursorType);
        final long numRows = sequentializedGraph.tableTransformGraph.numRows();
        System.out.println("numRows = " + numRows);
        return new CursorAssemblyPlan(builder.cap, cursorType, numRows, builder.sourceSchemas);
    }

    private int index = 0;

    private CapNode appendBranch(final BranchGraph.BranchEdge branch) {
        // append predecessor branches
        final List<CapNode> heads = new ArrayList<>();
        branch.target().branches().forEach(b -> heads.add(appendBranch(b)));

        // current head while building this branch
        CapNode capNode = null;

        // append branch target
        final TableTransformGraph.Node target = branch.target().node();
        switch (target.type()) {
            case SOURCE -> {
                final SourceTransformSpec spec = target.getTransformSpec();
                final UUID uuid = spec.getSourceIdentifier();
                final List<AccessId> outputs = target.out().accesses();
                final int[] columns = outputs.stream().mapToInt(a -> a.find().producer().index()).toArray();
                final Selection.RowRangeSelection range = spec.getRowRange();
                capNode = new CapNodeSource(index++, uuid, columns, range);
                createCapAccessIdsFor(outputs, capNode);
                append(target, capNode);
                sourceSchemas.put(uuid, spec.getSchema());
            }
            case APPEND -> {
                final int numPredecessors = heads.size();
                final int[] predecessors = new int[numPredecessors];
                final int[][] predecessorOutputIndices = new int[numPredecessors][];
                final long[] predecessorSizes = new long[numPredecessors];
                final List<AccessId> inputs = new ArrayList<>();
                for (int i = 0; i < numPredecessors; ++i) {
                    predecessors[i] = heads.get(i).index();
                    predecessorSizes[i] = TableTransformGraphProperties.numRows(target.in(i));
                    final List<AccessId> branchInputs = target.in(i).accesses();
                    predecessorOutputIndices[i] = new int[branchInputs.size()];
                    Arrays.setAll(predecessorOutputIndices[i], j -> j + inputs.size());
                    inputs.addAll(branchInputs);
                }
                final CapAccessId[] capInputs = capAccessIdsFor(inputs);
                capNode =
                        new CapNodeAppend(index++, capInputs, predecessors, predecessorOutputIndices, predecessorSizes);
                createCapAccessIdsFor(target.out().accesses(), capNode);
                append(target, capNode);
            }
            case CONCATENATE -> {
                final int numPredecessors = heads.size();
                final int[] predecessors = new int[numPredecessors];
                final CapAccessId[][] capInputs = new CapAccessId[numPredecessors][];
                final long[] predecessorSizes = new long[numPredecessors];
                for (int i = 0; i < numPredecessors; ++i) {
                    predecessors[i] = heads.get(i).index();
                    capInputs[i] = capAccessIdsFor(target.in(i).accesses());
                    predecessorSizes[i] = TableTransformGraphProperties.numRows(target.in(i));
                }
                capNode = new CapNodeConcatenate(index++, capInputs, predecessors, predecessorSizes);
                createCapAccessIdsFor(target.out().accesses(), capNode);
                append(target, capNode);
            }
            default -> throw new IllegalStateException();
        }

        // append inner nodes
        for (BranchGraph.AbstractNode depNode : branch.innerNodes()) {
            final TableTransformGraph.Node node = depNode.node();
            final List<AccessId> inputs = node.in(0).accesses();
            final CapAccessId[] capInputs = capAccessIdsFor(inputs);

            final int predecessor = capNode.index();
            switch (node.type()) {
                case MAP -> {
                    final MapTransformSpec spec = node.getTransformSpec();
                    final List<AccessId> outputs = node.out().accesses();
                    final int[] columns = outputs.stream().mapToInt(a -> a.find().producer().index()).toArray();
                    capNode = new CapNodeMap(index++, capInputs, predecessor, columns, spec.getMapperFactory());
                    createCapAccessIdsFor(outputs, capNode);
                    append(node, capNode);
                }
                case SLICE -> {
                    final SliceTransformSpec spec = node.getTransformSpec();
                    final long from = spec.getRowRangeSelection().fromIndex();
                    final long to = spec.getRowRangeSelection().toIndex();
                    capNode = new CapNodeSlice(index++, predecessor, from, to);
                    append(node, capNode);
                }
                case ROWFILTER -> {
                    final RowFilterTransformSpec spec = node.getTransformSpec();
                    capNode = new CapNodeRowFilter(index++, capInputs, predecessor, spec.getFilterFactory());
                    append(node, capNode);
                }
                case ROWINDEX -> {
                    final RowIndexTransformSpec spec = node.getTransformSpec();
                    final List<AccessId> outputs = node.out().accesses();
                    capNode = new CapNodeRowIndex(index++, predecessor, spec.getOffset());
                    createCapAccessIdsFor(outputs, capNode);
                    append(node, capNode);
                }
                case OBSERVER -> throw SpecGraph.unhandledNodeType();
                default -> throw new IllegalStateException();
            }
        }

        return capNode;
    }

    /**
     * Append the given {@code capNode} to the plan.
     * Remember the association to corresponding {@code ragNode}.
     */
    private void append(final TableTransformGraph.Node ragNode, final CapNode capNode) {
        cap.add(capNode);
        capNodes.put(ragNode, capNode);
    }

    /**
     * Create a new CapAccessId with the given producer and slot indices starting from 0 for the given {@code outputs} AccessIds.
     */
    private void createCapAccessIdsFor(final Iterable<AccessId> outputs, final CapNode producer) {
        int i = 0;
        for (AccessId output : outputs) {
            capAccessIds.put(output.find(), new CapAccessId(producer, i++));
        }
    }

    /**
     * Get CapAccessId[] array corresponding to (Rag)AccessIds collection.
     */
    private CapAccessId[] capAccessIdsFor(final Collection<AccessId> ids) {
        final CapAccessId[] imps = new CapAccessId[ids.size()];
        int i = 0;
        for (AccessId id : ids) {
            imps[i++] = capAccessIds.get(id.find());
        }
        return imps;
    }

}
