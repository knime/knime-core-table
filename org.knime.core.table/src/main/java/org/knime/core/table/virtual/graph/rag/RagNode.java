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

import static org.knime.core.table.virtual.graph.rag.RagEdgeType.EXEC;
import static org.knime.core.table.virtual.graph.rag.RagEdgeType.SPEC;
import static org.knime.core.table.virtual.graph.rag.RagNodeType.APPEND;
import static org.knime.core.table.virtual.graph.rag.RagNodeType.CONCATENATE;
import static org.knime.core.table.virtual.graph.rag.RagNodeType.MISSING;
import static org.knime.core.table.virtual.graph.rag.RagNodeType.SLICE;
import static org.knime.core.table.virtual.graph.rag.RagNodeType.SOURCE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

import org.knime.core.table.virtual.TableTransform;
import org.knime.core.table.virtual.spec.TableTransformSpec;

public final class RagNode implements Typed<RagNodeType> {

    private int numColumns = -1;
    private final RagNodeType type;
    private final TableTransformSpec transformSpec;

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
     * nodes, etc., neither require nor produce any accesses.
     */
    private AccessIds[] inputs;

    /**
     * TODO (TP) javadoc
     */
    public static class AccessValidity {// TODO (TP) move to separate file

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

    // TODO: move to RagNodeType ?
    private static final EnumSet<RagNodeType> validityProvidingNodeTypes = EnumSet.of(SOURCE, APPEND, CONCATENATE, MISSING);

    private final AccessValidity validity;

    /**
     * Create a node corresponding to a transform with the given {@code spec}.
     * <p>
     * For CONCATENATE nodes, {@code numInputsArrays} gives the number of SPEC
     * predecessors that connect to the node (arrays of input {@code
     * ReadAccess}es to switch between). All other transform types should give
     * {@code numInputsArrays=1}.
     */
    RagNode(final TableTransformSpec spec, final int numInputsArrays) {
        transformSpec = spec;
        type = RagNodeType.forSpec(transformSpec);
        validity = validityProvidingNodeTypes.contains(type) ? new AccessValidity(this) : null;
        outputs = new AccessIds();

        inputs = new AccessIds[numInputsArrays];
        Arrays.setAll(inputs, i -> new AccessIds());

        id = nextNodeId++;
    }

    RagNode(final TableTransform transform) {
        this(transform.getSpec(),//
                (RagNodeType.forSpec(transform.getSpec()) == RagNodeType.CONCATENATE)//
                        ? transform.getPrecedingTransforms().size() //
                        : 1);
    }

    @Override
    public RagNodeType type() {
        return type;
    }

    public <T extends TableTransformSpec> T getTransformSpec() {
        return (T)transformSpec;
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
     * The number of rows is computed recursively, by tracing EXEC edges
     * backwards to SOURCE nodes.
     *
     * @return the number of rows, or a negative number if the number of rows is unknown.
     */
    // TODO (TP) inline and remove?
    public long numRows() {
        return RagGraphProperties.numRows(this);
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
        sb.append(String.format("spec=\"%s\"", transformSpec));
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
     * TODO (TP) javadoc
     */
    public AccessValidity validity() {// TODO (TP) rename?
        if (validity != null) {
            return validity;
        } else {
            List<RagEdge> predecessorEdges = incoming.unmodifiable(EXEC);
            if (predecessorEdges.isEmpty())
                predecessorEdges = incoming.unmodifiable(SPEC);
            return predecessorEdges.get(0).getSource().validity();

        }
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
        if (columnIndex < 0)
            throw new IndexOutOfBoundsException(columnIndex);
        else if (columnIndex > numColumns)
            throw new IndexOutOfBoundsException(columnIndex);
        else if (columnIndex == numColumns) {
            if (type == MISSING)
                numColumns++;
            else
                throw new IndexOutOfBoundsException(columnIndex);
        }
        AccessId id = outputs.getAtColumnIndex(columnIndex);
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

    void setInputssArray(final AccessIds[] inputs) {
        this.inputs = inputs;
    }

    /**
     * Replace all uses of {@code oldId} as an input to this node with {@code newId}.
     * <p>
     * Note that neither the consumers of {@code oldId} nor the consumers of {@code
     * newId} are updated. This avoids {@code ConcurrentModificationException}s when
     * calling {@code replaceInput} in a loop over all consumers of {@code oldId}.
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
