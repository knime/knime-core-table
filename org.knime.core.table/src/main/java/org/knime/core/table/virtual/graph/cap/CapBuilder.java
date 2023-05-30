package org.knime.core.table.virtual.graph.cap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.knime.core.table.row.Selection.RowRangeSelection;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.DataSpecs.DataSpecWithTraits;
import org.knime.core.table.virtual.graph.cap.Branches.Branch;
import org.knime.core.table.virtual.graph.rag.AccessId;
import org.knime.core.table.virtual.graph.rag.AccessIds;
import org.knime.core.table.virtual.graph.rag.MissingValuesSourceTransformSpec;
import org.knime.core.table.virtual.graph.rag.RagBuilder;
import org.knime.core.table.virtual.graph.rag.RagNode;
import org.knime.core.table.virtual.spec.MapTransformSpec;
import org.knime.core.table.virtual.spec.MaterializeTransformSpec;
import org.knime.core.table.virtual.spec.RowFilterTransformSpec;
import org.knime.core.table.virtual.spec.SliceTransformSpec;
import org.knime.core.table.virtual.spec.SourceTransformSpec;

public class CapBuilder {

    public static CursorAssemblyPlan createCursorAssemblyPlan(final List<RagNode> orderedRag) {
        return new CapBuilder(orderedRag).createCAP();
    }

    private final List<RagNode> orderedRag;

    private final Map<RagNode, CapNode> capNodes;
    private final Map<AccessId, CapAccessId> capAccessIds;
    private final Branches branches;
    private final List<CapNode> cursorAssemblyPlan;

    private CapBuilder(final List<RagNode> orderedRag) {
        this.orderedRag = orderedRag;
        final int cap = orderedRag.size() * 4 / 3;
        branches = new Branches(cap);
        capNodes = new HashMap<>(cap);
        capAccessIds = new HashMap<>();
        cursorAssemblyPlan = new ArrayList<>(orderedRag.size());
    }

    private CursorAssemblyPlan createCAP() {
        for (int index = 0; index < orderedRag.size(); index++) {
            final RagNode node = orderedRag.get(index);
            switch (node.type()) {
                case SOURCE: {
                    final SourceTransformSpec spec = node.getTransformSpec();
                    final UUID uuid = spec.getSourceIdentifier();
                    final Collection<AccessId> outputs = node.getOutputs();
                    final RowRangeSelection range = spec.getRowRange();
                    final ColumnarSchema schema = spec.getSchema();
                    final CapNodeSource capNode = new CapNodeSource(index, uuid, columnIndicesFor(outputs), range);
                    append(node, capNode);
                    createCapAccessIdsFor(outputs, capNode);
                    branches.createBranch(node);
                    break;
                }
                case MISSING: {
                    final MissingValuesSourceTransformSpec spec = node.getTransformSpec();
                    final List<DataSpec> missingValueSpecs = spec.getMissingValueSpecs().stream()
                            .map(DataSpecWithTraits::spec)
                            .toList();
                    final CapNodeMissing capNode = new CapNodeMissing(index, missingValueSpecs);
                    append(node, capNode);
                    createCapAccessIdsFor(node.getOutputs(), capNode);
                    // N.B. The only reason that MISSING is on a branch is for sorting inputs by predecessor in APPEND.
                    branches.createBranch(node);
                    break;
                }
                case SLICE: {
                    final SliceTransformSpec spec = node.getTransformSpec();
                    final long from = spec.getRowRangeSelection().fromIndex();
                    final long to = spec.getRowRangeSelection().toIndex();
                    final Branch branch = branches.getPredecessorBranch(node);
                    final CapNodeSlice capNode = new CapNodeSlice(index, headIndex(branch), from, to);
                    append(node, capNode);
                    branch.append(node);
                    break;
                }
                case ROWFILTER: {
                    final RowFilterTransformSpec spec = node.getTransformSpec();
                    final Branch branch = branches.getPredecessorBranch(node);
                    final CapAccessId[] inputs = capAccessIdsFor(node.getInputs());
                    final CapNodeRowFilter capNode =
                            new CapNodeRowFilter(index, inputs, headIndex(branch), spec.getFilterFactory());
                    append(node, capNode);
                    branch.append(node);
                    break;
                }
                case MAP: {
                    final MapTransformSpec spec = node.getTransformSpec();
                    final Branch branch = branches.getPredecessorBranch(node);
                    final CapAccessId[] inputs = capAccessIdsFor(node.getInputs());
                    final Collection<AccessId> outputs = node.getOutputs();
                    final int[] cols = columnIndicesFor(outputs);// column indices of map() outputs that are consumed
                    final CapNodeMap capNode =
                            new CapNodeMap(index, inputs, headIndex(branch), cols, spec.getMapperFactory());
                    append(node, capNode);
                    createCapAccessIdsFor(outputs, capNode);
                    branch.append(node);
                    break;
                }
                case APPEND: {
                    // Each predecessor is the head of an incoming branch.
                    final List<Branch> predecessorBranches = branches.getPredecessorBranches(node);
                    final int[] predecessors = headIndices(predecessorBranches);

                    // Input AccessId[] come from node.getInputs().
                    // Output AccessId[] come from node.getOutputs().
                    // Input and output are linked by corresponding indices.
                    //
                    // For every input, we find out which branch the input access belongs to,
                    // the index of that branch in this node's predecessors[].
                    //
                    // A slot in the CapNodeAppend outputs is then linked to a
                    // (input AccessId, output AccessId, predecessor) triple.
                    // We order these slots by predecessor index.
                    //
                    // N.B. The re-ordering is not yet important but will be used in the future
                    // to represent CapNodeAppend more compactly.
                    final AccessId[] ragInputs = node.getInputs().toArray(AccessId[]::new);
                    final AccessId[] ragOutputs = node.getOutputs().toArray(AccessId[]::new);
                    final int numSlots = ragInputs.length;
                    final int[] slotPredecessorIndices = new int[numSlots];
                    Arrays.setAll(slotPredecessorIndices,
                            i -> predecessorBranches.indexOf(branches.getBranch(ragInputs[i].getProducer())));
                    class Slot {
                        final AccessId ragInput;

                        final AccessId ragOutput;

                        final int predecessorIndex;

                        Slot(final AccessId ragInput, final AccessId ragOutput, final int predecessorIndex) {
                            this.ragInput = ragInput;
                            this.ragOutput = ragOutput;
                            this.predecessorIndex = predecessorIndex;
                        }

                        int predecessorIndex() {
                            return predecessorIndex;
                        }
                    }
                    final Slot[] slots = new Slot[numSlots];
                    Arrays.setAll(slots, i -> new Slot(ragInputs[i], ragOutputs[i], slotPredecessorIndices[i]));
                    Arrays.sort(slots, Comparator.comparing(Slot::predecessorIndex));

                    final CapAccessId[] inputs = new CapAccessId[numSlots];
                    Arrays.setAll(inputs, i -> capAccessIds.get(slots[i].ragInput));

                    final int[] predecessorColRange = new int[predecessors.length + 1];
                    int j = 0;
                    for (int i = 0; i < slots.length; i++) {
                        final int p = slots[i].predecessorIndex;
                        while (j <= p) {
                            predecessorColRange[j++] = i;
                        }
                    }
                    while (j <= predecessors.length) {
                        predecessorColRange[j++] = slots.length;
                    }

                    final int[][] predecessorOutputIndices = new int[predecessors.length][];
                    Arrays.setAll(predecessorOutputIndices, p -> {
                        final int from = predecessorColRange[p];
                        final int to = predecessorColRange[p + 1];
                        final int[] indices = new int[to - from];
                        Arrays.setAll(indices, i -> from + i);
                        return indices;
                    });

                    final CapNodeAppend capNode =
                            new CapNodeAppend(index, inputs, predecessors, predecessorOutputIndices);
                    append(node, capNode);

                    // TODO: Set capAccessIds to point to the CapNodeAppend outputs only if new delegating accesses are created
                    //  For now, delegating accesses are always created.
                    //  Later this can be refined:
                    //  1.) by not wrapping again accesses that come from a wrapping APPEND already.
                    //      In this case, just put input CapAccessId from the corresponding index. (?)
                    for (int i = 0; i < slots.length; i++) {
                        capAccessIds.put(slots[i].ragOutput, new CapAccessId(capNode, i));
                    }

                    merge(predecessorBranches).append(node);
                    break;
                }
                case CONCATENATE: {
                    // Each predecessor is the head of an incoming branch.
                    final List<Branch> predecessorBranches = branches.getPredecessorBranches(node);
                    final int[] predecessors = headIndices(predecessorBranches);

                    final AccessIds[] inputAccessIdss = node.getInputssArray();
                    final CapAccessId[][] inputs = new CapAccessId[inputAccessIdss.length][];
                    Arrays.setAll(inputs, i -> capAccessIdsFor(inputAccessIdss[i]));

                    final CapNodeConcatenate capNode = new CapNodeConcatenate(index, inputs, predecessors);
                    append(node, capNode);
                    createCapAccessIdsFor(node.getOutputs(), capNode);
                    merge(predecessorBranches).append(node);
                    break;
                }
                case CONSUMER: {
                    final Branch branch = branches.getPredecessorBranch(node);
                    final CapAccessId[] inputs = capAccessIdsFor(node.getInputs());
                    final CapNodeConsumer capNode = new CapNodeConsumer(index, inputs, headIndex(branch));
                    append(node, capNode);
                    branch.append(node);
                    break;
                }
                case MATERIALIZE: {
                    final MaterializeTransformSpec spec = node.getTransformSpec();
                    final UUID uuid = spec.getSinkIdentifier();
                    final Branch branch = branches.getPredecessorBranch(node);
                    final CapAccessId[] inputs = capAccessIdsFor(node.getInputs());
                    final CapNodeMaterialize capNode = new CapNodeMaterialize(index, uuid, inputs, headIndex(branch));
                    append(node, capNode);
                    branch.append(node);
                    break;
                }
                case COLFILTER:
                case APPENDMISSING:
                case WRAPPER:
                    throw new IllegalArgumentException(
                            "Unexpected RagNode type " + node.type() + ". This node should not be present in the processed RAG.");
                default:
                    throw new IllegalStateException("Unexpected value: " + node.type());
            }
        }

        return new CursorAssemblyPlan(cursorAssemblyPlan, //
                RagBuilder.supportsLookahead(orderedRag), //
                RagBuilder.numRows(orderedRag), //
                RagBuilder.getSourceAndSinkSchemas(orderedRag));
    }

    /**
     * Append the given {@code capNode} to the plan.
     * Remember the association to corresponding {@code ragNode}.
     */
    private void append(final RagNode ragNode, final CapNode capNode) {
        cursorAssemblyPlan.add(capNode);
        capNodes.put(ragNode, capNode);
    }

    /**
     * Create a new CapAccessId with the given producer and slot indices starting from 0 for the given {@code outputs} AccessIds.
     */
    private void createCapAccessIdsFor(final Iterable<AccessId> outputs, final CapNode producer) {
        int i = 0;
        for (AccessId output : outputs) {
            capAccessIds.put(output, new CapAccessId(producer, i++));
        }
    }

    /**
     * Get CapAccessId[] array corresponding to (Rag)AccessIds collection.
     */
    private CapAccessId[] capAccessIdsFor(final Collection<AccessId> ids) {
        final CapAccessId[] imps = new CapAccessId[ids.size()];
        int i = 0;
        for (AccessId id : ids) {
            imps[i++] = capAccessIds.get(id);
        }
        return imps;
    }

    /**
     * Get int[] array of column indices of the given (Rag)AccessIds.
     */
    private int[] columnIndicesFor(final Collection<AccessId> ids) {
        final int[] cols = new int[ids.size()];
        int i = 0;
        for (AccessId id : ids) {
            cols[i++] = id.getColumnIndex();
        }
        return cols;
    }

    /**
     * Maps the head (RagNode) of the given branch to the corresponding CapNode index.
     */
    private int headIndex(final Branch branch) {
        return capNodes.get(branch.head).index();
    }

    /**
     * Maps the heads (RagNodes) of the given branches to corresponding CapNode indices.
     */
    private int[] headIndices(final List<Branch> branches) {
        final int[] predecessors = new int[branches.size()];
        Arrays.setAll(predecessors, i -> capNodes.get(branches.get(i).head).index());
        return predecessors;
    }

    /**
     * Merges all other given branches into the first one.
     * @return merged branch
     */
    private Branch merge(final List<Branch> branches)
    {
        final Branch main = branches.get(0);
        for (int j = 1; j < branches.size(); j++) {
            main.merge(branches.get(j));
        }
        return main;

    }
}
