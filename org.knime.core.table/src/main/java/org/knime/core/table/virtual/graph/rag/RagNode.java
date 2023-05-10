package org.knime.core.table.virtual.graph.rag;

import static org.knime.core.table.virtual.graph.rag.RagNodeType.MISSING;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.knime.core.table.virtual.TableTransform;
import org.knime.core.table.virtual.spec.TableTransformSpec;

public final class RagNode implements Typed<RagNodeType> {

    private int numColumns = -1;
    private long numRows = -1;
    private final RagNodeType type;
    private final TableTransform transform;

    final TypedObjects<RagEdgeType, RagEdge> outgoing = new TypedObjects<>(RagEdgeType.class);
    final TypedObjects<RagEdgeType, RagEdge> incoming = new TypedObjects<>(RagEdgeType.class);

    /**
     * Can be used by algorithms (e.g. topological sort)
     */
    private int mark;

    /**
     * Accesses produced by this node.
     * <p>
     * An access is only "produced" if the node actually creates (or buffers) the value.
     * That is, column permutation nodes, etc., neither require nor produce any accesses.
     */
    private final AccessIds outputs;

    /**
     * Accesses required by this node.
     * <p>
     * An access is only "required" if its value is used. That is, column permutation
     * nodes, etc., neither require or produce any accesses.
     */
    private AccessIds[] inputs;

    RagNode(final TableTransform transform) {
        this.transform = transform;
        type = RagNodeType.forSpec(transform.getSpec());
        outputs = new AccessIds();

        final int numInputsArrays = (type == RagNodeType.CONCATENATE) ? transform.getPrecedingTransforms().size() : 1;
        inputs = new AccessIds[numInputsArrays];
        Arrays.setAll(inputs, i -> new AccessIds());

        id = nextNodeId++;
    }

    @Override
    public RagNodeType type() {
        return type;
    }

    public <T extends TableTransformSpec> T getTransformSpec() {
        return (T)transform.getSpec();
    }

    public int numColumns() {
        return numColumns;
    }

    // TODO It would be better if this could just be initialized on construction, but...
    //   Because of the way nested VirtualTable hierarchies are implemented now, we have
    //   to infer the number of columns after the spec graph is constructed, working back
    //   from the SourceTableTransforms. This could be improved by making all
    //   TableTransforms know their schema or at least numColumns
    public void setNumColumns(int numColumns) {
        this.numColumns = numColumns;
    }

    /**
     * Get the number of rows at this node.
     * <p>
     * The number of rows is computed by {@link RagBuilder#numRows(List)} for
     * all executable nodes (that is, those linked with {@link RagEdgeType#EXEC
     * EXEC} edges, not {@link RagNodeType#MAP MAP}, {@link
     * RagNodeType#COLFILTER ROWFILTER}, etc).
     *
     * @return the number of rows, or a negative number if the number of rows is unknown.
     */
    public long numRows() {
        return numRows;
    }

    public void setNumRows(long numRows) {
        this.numRows = numRows;
    }

    /**
     * Returns all outgoing edges of the specified {@code edgeType}. If {@code edgeType
     * == null}, all outgoing edges (of any type) are returned.
     */
    public Collection<RagEdge> outgoingEdges(RagEdgeType edgeType) {
        return outgoing.unmodifiable(edgeType);
    }

    /**
     * Returns all incoming edges of the specified {@code edgeType}. If {@code edgeType
     * == null}, all incoming edges (of any type) are returned.
     */
    public Collection<RagEdge> incomingEdges(RagEdgeType edgeType) {
        return incoming.unmodifiable(edgeType);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append(String.format("<%d>, ", id));
        sb.append(String.format("numColumns=%d, ", numColumns));
        sb.append(String.format("spec=\"%s\"", transform.getSpec()));
        sb.append(", outputs=").append(outputs);
        sb.append(", inputs=").append(Arrays.toString(inputs));
        sb.append(", mark=").append(mark);
        sb.append("}");
        return sb.toString();
    }

    /**
     * Get the unique predecessor of the specified {@code edgeType}.
     * (A predecessor is the source node of an incoming edge of the specified {@code edgeType}.)
     *
     * @throws IllegalArgumentException if there is no unique predecessor of the given type.
     */
    public RagNode predecessor(final RagEdgeType edgeType) {
        final List<RagNode> predecessors = predecessors(edgeType);
        if (predecessors.size() != 1)
            throw new IllegalArgumentException();
        return predecessors.get(0);
    }

    /**
     * Get predecessors of the specified {@code edgeType}.
     * (A predecessor is the source node of an incoming edge of the specified {@code edgeType}.)
     */
    public List<RagNode> predecessors(final RagEdgeType edgeType) {
        final List<RagNode> predecessors = new ArrayList<>();
        incomingEdges(edgeType).forEach(e -> predecessors.add(e.getSource()));
        return predecessors;
    }

    /**
     * Get the unique successor of the specified {@code edgeType}.
     * (A successor is the target node of an outgoing edge of the specified {@code edgeType}.)
     *
     * @throws IllegalArgumentException if there is no unique successor of the given type.
     */
    public RagNode successor(final RagEdgeType edgeType) {
        final List<RagNode> successors = successors(edgeType);
        if (successors.size() != 1)
            throw new IllegalArgumentException();
        return successors.get(0);
    }

    /**
     * Get successors of the specified {@code edgeType}.
     * (A successor is the target node of an outgoing edge of the specified {@code edgeType}.)
     */
    public List<RagNode> successors(final RagEdgeType edgeType) {
        final List<RagNode> successors = new ArrayList<>();
        outgoingEdges(edgeType).forEach(e -> successors.add(e.getTarget()));
        return successors;
    }

    /**
     * Get all output {@code AccessId} produced by this node.
     * The returned collection is sorted by {@code columnIndex}.
     * <p>
     * An access is only "produced" if the node actually creates (or buffers) the value.
     * That is, column permutation nodes, etc., neither require nor produce any accesses.
     */
    public AccessIds getOutputs() {
        return outputs;
    }

    /**
     * Get the output {@code AccessId} for {@code columnIndex} or create it if it
     * doesn't exist yet. This should be only called when tracing accesses during IR
     * graph building.
     * <p>
     * An access is only "produced" if the node actually creates (or buffers) the
     * value. That is, column permutation nodes, etc., neither require nor produce any
     * accesses.
     */
    AccessId getOrCreateOutput(final int columnIndex) {
        if (columnIndex < 0 )
            throw new IndexOutOfBoundsException(columnIndex);
        else if (columnIndex > numColumns)
            throw new IndexOutOfBoundsException(columnIndex);
        else if (columnIndex == numColumns ) {
            if (type == MISSING)
                numColumns++;
            else
                throw new IndexOutOfBoundsException(columnIndex);
        }
        AccessId id = outputs.getAtColumnIndex( columnIndex );
        if (id == null) {
            id = new AccessId(this, columnIndex);
            outputs.putAtColumnIndex(id, columnIndex);
        }
        return id;
    }

    /**
     * Get the input {@code AccessIds} required by this node (expect for {@link
     * RagNodeType#CONCATENATE CONCATENATE} nodes, see below).
     * <p>
     * An access is only "required" if its value is used. That is, column permutation
     * nodes, etc., neither require nor produce any accesses.
     * <p>
     * This method returns the first input {@link AccessIds} of this node, it is just a
     * shortcut for {@code getInputs(0)}. All {@code NodeType}s have exactly one input
     * {@link AccessIds}, except {@code CONCATENATE}. {@code CONCATENATE} has one input
     * {@link AccessIds} for each concatenated table.
     */
    public AccessIds getInputs() {
        return inputs[0];
    }

    /**
     * Get the input {@code AccessIds} required by this node, at {@code predecessorIndex}.
     * <p>
     * An access is only "required" if its value is used. That is, column permutation
     * nodes, etc., neither require nor produce any accesses.
     * <p>
     * All {@code NodeType}s have exactly one input {@link AccessIds}, that is {@code
     * getInputs(0)} or equivalently {@code getInputs()}. The exception is {@code
     * CONCATENATE} which has one input {@link AccessIds} for each concatenated table.
     */
    public AccessIds getInputs(final int predecessorIndex) {
        return inputs[predecessorIndex];
    }

    /**
     * Get an array of all input {@code AccessIds} sets required by this node.
     * <p>
     * An access is only "required" if its value is used. That is, column permutation
     * nodes, etc., neither require nor produce any accesses.
     * <p>
     * All {@code NodeType}s have exactly one input {@link AccessIds}, that is {@code
     * getInputs(0)} or equivalently {@code getInputs()}. The exception is {@code
     * CONCATENATE} which has one input {@link AccessIds} for each concatenated table.
     */
    public AccessIds[] getInputssArray() {
        return inputs;
    }

    void setInputssArray( final AccessIds[] inputs ) {
        this.inputs = inputs;
    }

    /**
     * Replace all uses of {@code oldId} as an input to this node with {@code newId}.
     * <p>
     * Note that neither the consumers of {@code oldId} nor the consumers of {@code
     * newId} are updated.
     *
     * @param oldId access to be replaced
     * @param newId access to replace it with
     * @return {@code true} iff any replacement was made (that is, iff {@code oldId} occurred as an input to this node)
     */
    public boolean replaceInput(final AccessId oldId, final AccessId newId) {
        boolean replaced = false;
        for (AccessIds ids : inputs) {
            replaced |= ids.replace(oldId, newId);
        }
        return replaced;
    }

    public int getMark() {
        return mark;
    }

    public void setMark(int mark) {
        this.mark = mark;
    }


    // ------------------------------------------------------------------------
    //   debugging stuff


    // ids are just for printing ...
    private static int nextNodeId = 0;
    private final int id;

    /**
     * To make it easier to identify nodes in debug output, each node is assigned a
     * unique id on construction.
     * <p>
     * <em>This might be removed later. Please do not rely on uniqueness of {@code
     * id()}!</em>
     */
    public int id() {
        return id;
    }
}
