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
 *
 * History
 *   3 Dec 2024 (pietzsch): created
 */
package org.knime.core.table.virtual.graph.rag;

import static org.knime.core.table.virtual.graph.rag.SpecType.ROWFILTER;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.IntUnaryOperator;

import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.TableTransform;
import org.knime.core.table.virtual.graph.rag.prettyprint.DependencyGraph;
import org.knime.core.table.virtual.spec.AppendMapTransformSpec;
import org.knime.core.table.virtual.spec.AppendMissingValuesTransformSpec;
import org.knime.core.table.virtual.spec.MapTransformSpec;
import org.knime.core.table.virtual.spec.ObserverTransformSpec;
import org.knime.core.table.virtual.spec.RowFilterTransformSpec;
import org.knime.core.table.virtual.spec.SelectColumnsTransformSpec;
import org.knime.core.table.virtual.spec.SourceTableProperties.CursorType;
import org.knime.core.table.virtual.spec.SourceTransformSpec;
import org.knime.core.table.virtual.spec.TableTransformSpec;

/**
 * Graph representation of a virtual table {@code TableTransform}.
 */
public class TableTransformGraph {

    /**
     * Bundle incoming/outgoing accesses and control-flow edges.
     * <p>
     * Every {@code Node} has exactly one {@link Node#out() out} port.
     * <p>
     * Every {@code Node} has exactly one {@link Node#in() in} port, except SOURCE which has none, and
     * APPEND/CONCATENATE which have one or more.
     *
     * @param owner the node which this port belongs to (as in or out port)
     * @param accesses the input or output accesses (depending on whether this is an in or out port)
     * @param controlFlowEdges control-flow edges from or to this port (depending on whether this is an in or out port)
     */
    public record Port(Node owner, List<AccessId> accesses, List<ControlFlowEdge> controlFlowEdges) {
        Port(final Node owner, final List<AccessId> accesses) {
            this(owner, accesses, new ArrayList<>());
        }

        Port(final Node owner) {
            this(owner, new ArrayList<>());
        }

        AccessId access(final int i) {
            return accesses.get(i);
        }

        Node controlFlowTarget(final int i) {
            return controlFlowEdges.get(i).to().owner();
        }

        Node controlFlowSource(final int i) {// TODO: currently unused. remove?
            return controlFlowEdges.get(i).from().owner();
        }

        /**
         * Performs the given action for each of {@code ControlFlowEdge}.
         * Calls {@code forEach(action)} on a copy of the {@code controlFlowEdges} list,
         * such that , on a copy of the
         * R
         * @param action
         */
        public void forEachControlFlowEdge(final Consumer<? super ControlFlowEdge> action) {
            new ArrayList<>(controlFlowEdges).forEach(action);
        }

        /**
         * Add a new control-flow edge from this port to the {@link Node#out() out} port of the given {@code Node}.
         * <p>
         * <em>This should be only called on an {@link Node#in() in} port!</em>
         */
        void linkTo(final Node to) {
            final ControlFlowEdge e = new ControlFlowEdge(this, to.out());
            controlFlowEdges.add(e);
            to.out().controlFlowEdges().add(e);
        }

        @Override
        public boolean equals(final Object obj) {
            return obj == this;
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(this);
        }
    }

    public record ControlFlowEdge(Port from, Port to) {
        /**
         * Remove this {@code ControlFlowEdge} from its source {@code Port} and
         * replace it in its target {@code Port} with a new edge from {@code
         * from}.
         *
         * @return the new edge
         */
        ControlFlowEdge relinkFrom(final Port from) throws IllegalStateException {
            this.from.controlFlowEdges.remove(this);
            final int i = this.to.controlFlowEdges.indexOf(this);
            if (i < 0) {
                throw new IllegalStateException();
            }
            final var e = new ControlFlowEdge(from, this.to);
            this.to.controlFlowEdges.set(i, e);
            from.controlFlowEdges.add(e);
            return e;
        }

        /**
         * Remove this {@code ControlFlowEdge} from its target {@code Port} and
         * replace it in its source {@code Port} with a new edge to {@code to}.
         *
         * @return the new edge
         */
        ControlFlowEdge relinkTo(final Port to) throws IllegalStateException {
            this.to.controlFlowEdges.remove(this);
            final int i = this.from.controlFlowEdges.indexOf(this);
            if (i < 0) {
                throw new IllegalStateException();
            }
            final var e = new ControlFlowEdge(this.from, to);
            this.from.controlFlowEdges.set(i, e);
            to.controlFlowEdges.add(e);
            return e;
        }

        /**
         * Remove this {@code ControlFlowEdge} from both its source and target
         * {@code Port}.
         */
        void remove() {
            this.to.controlFlowEdges.remove(this);
            this.from.controlFlowEdges.remove(this);
        }

        @Override
        public String toString() {
            return "ControlFlowEdge{" + //
                    "from=" + (from.owner() == null ? "terminal" : from.owner().id()) + //
                    ", to=" + to.owner().id() + "}";
        }
    }

    /**
     * A node in the TableTransformGraph.
     */
    public static class Node {

        // ids are just for printing ...
        private static int nextNodeId = 1;
        private final int id;

        private final TableTransformSpec spec;
        private final SpecType type;
        private final List<Port> in = new ArrayList<>();
        private final Port out;


        /**
         * Create a new node with the given {@code TableTransformSpec} but without populated inputs or outputs.
         * This is needed to implement {@link #copy()}.
         */
        Node(final TableTransformSpec spec) {

            id = nextNodeId;
            ++nextNodeId;

            this.spec = spec;
            type = SpecType.forSpec(spec);
            out = new Port(this);
        }

        private Node(final TableTransformSpec spec, final int numOutputs,
            final List<TableTransformGraph> predecessors) {

            id = nextNodeId;
            ++nextNodeId;

            this.spec = spec;
            type = SpecType.forSpec(spec);

            for (int p = 0; p < predecessors.size(); p++) {
                final TableTransformGraph predecessor = predecessors.get(p);
                final int numInputs = switch (type) {
                    case SOURCE, SLICE, ROWINDEX -> 0;
                    case MAP, ROWFILTER, OBSERVER -> getColumnSelection(spec).length;
                    case APPEND, CONCATENATE -> predecessor.numColumns();
                    case COLSELECT, APPENDMAP, APPENDMISSING -> throw new IllegalArgumentException();
                };

                final List<AccessId> inputs = createAccessIds(null, numInputs, accessLabel("gamma", id, p));
                final Port inPort = new Port(this, inputs);

                // access tracing:
                // link inputs to predecessor outCols
                switch (type) {
                    case MAP, ROWFILTER -> {
                        final int[] selection = getColumnSelection(spec);
                        unionAccesses(inPort, predecessor.terminal, numInputs, i -> selection[i]);
                    }
                    default -> unionAccesses(inPort, predecessor.terminal, numInputs);
                }

                // control flow:
                switch (type) {
                    case ROWFILTER -> { // NOSONAR
                        final ControlFlowEdge predecessorEdge = predecessor.terminal.controlFlowEdges().get(0);
                        final Node predecessorNode = predecessorEdge.to().owner();
                        if (predecessorNode.type == ROWFILTER) {
                            // if predecessor links to a ROWFILTER (one or more)
                            // link this node to the target of that ROWFILTER's controlFlowEdge
                            final Node target = predecessorNode.in(0).controlFlowTarget(0);
                            inPort.linkTo(target);
                        } else {
                            // otherwise re-link the predecessor controlFlowEdge
                            // (there is only one) to this node
                            predecessorEdge.relinkFrom(inPort);
                        }
                    }
                    case SLICE, ROWINDEX, APPEND, CONCATENATE, OBSERVER -> {
                        // re-link the predecessor controlFlowEdges to this Node
                        predecessor.terminal.forEachControlFlowEdge(e -> e.relinkFrom(inPort));
                    }
                    default -> {
                        // NOSONAR
                    }
                }

                in.add(inPort);
            }

            final List<AccessId> outputs = createAccessIds(this, numOutputs, accessLabel("delta", id, -1)); // NOSONAR
            out = new Port(this, outputs);
        }

        public SpecType type() {
            return type;
        }

        public List<Port> in() {
            return in;
        }

        public Port in(final int i) {
            return in.get(i);
        }

        public Port out() {
            return out;
        }

        public <T extends TableTransformSpec> T getTransformSpec() {
            return (T)spec;
        }

        @Override
        public String toString() {
            return "(<" + id + ">, " + type + ", " + spec + ")";
        }

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

    private final Port terminal;

    TableTransformGraph(final Port terminal) {
        this.terminal = terminal;
    }

    /**
     * Build a {@code TableTransformGraph} from {@code tableTransform}.
     *
     * @param tableTransform the producingTransform of the table
     * @return TableTransformGraph  representing the given table
     */
    public TableTransformGraph(final TableTransform tableTransform) {
        this(tableTransform.getSpec(),
                tableTransform.getPrecedingTransforms().stream().map(TableTransformGraph::new).toList());
    }

    TableTransformGraph(final TableTransformSpec spec, final List<TableTransformGraph> predecessors) { // NOSONAR This method is complex, but splitting it up will not make it easier to understand.
        final SpecType type = SpecType.forSpec(spec);

        final int numOutputs = switch (type) {
            case SOURCE -> ((SourceTransformSpec)spec).getSchema().numColumns();
            case MAP -> ((MapTransformSpec)spec).getMapperFactory().getOutputSchema().numColumns();
            case APPENDMAP -> ((AppendMapTransformSpec)spec).getMapperFactory().getOutputSchema().numColumns();
            case APPENDMISSING -> ((AppendMissingValuesTransformSpec)spec).getAppendedSchema().numColumns();
            case ROWINDEX -> 1;
            case APPEND -> predecessors.stream().mapToInt(TableTransformGraph::numColumns).sum();
            case CONCATENATE -> predecessors.get(0).numColumns();
            case SLICE, ROWFILTER, COLSELECT, OBSERVER -> 0;
        };

        final int numColumns = switch (type) {
            case SOURCE, MAP, CONCATENATE, APPEND -> numOutputs;
            case APPENDMAP, APPENDMISSING, ROWINDEX, SLICE, ROWFILTER, OBSERVER -> //
                    numOutputs + predecessors.get(0).numColumns();
            case COLSELECT -> getColumnSelection(spec).length;
        };

        final List<AccessId> accessIds = createAccessIds(null, numColumns, i -> "beta^" + i);
        final List<ControlFlowEdge> controlFlowEdges = new ArrayList<>();
        terminal = new Port(null, accessIds, controlFlowEdges);

        final Node node = switch (type) {
            case COLSELECT -> // don't create a new node, just permute predecessor's outCols
                    null;
            case APPENDMAP -> // create a node with the equivalent MapTransformSpec
                    new Node(((AppendMapTransformSpec)spec).toMap(), numOutputs, predecessors);
            case APPENDMISSING -> // create a node with the equivalent MapTransformSpec
                    new Node(((AppendMissingValuesTransformSpec)spec).toMap(), numOutputs, predecessors);
            default -> new Node(spec, numOutputs, predecessors);
        };

        // access tracing:
        final Port predecessorTerminal = predecessors.isEmpty() ? null : predecessors.get(0).terminal;
        switch (type) {
            case SOURCE, MAP, APPEND, CONCATENATE -> {
                // link outCols to node's outputs
                unionAccesses(terminal, node.out, numColumns); // NOSONAR node cannot be null here
            }
            case SLICE, ROWFILTER, OBSERVER -> {
                // link outCols to the predecessor's outCols
                // (there is exactly one predecessor)
                unionAccesses(terminal, predecessorTerminal, numColumns);
            }
            case APPENDMAP, APPENDMISSING, ROWINDEX -> {
                // pass through the predecessor's outCols
                // (there is exactly one predecessor)
                unionAccesses(terminal, predecessorTerminal, numColumns - numOutputs);
                // link the final numOutputs outCols to the node's outputs
                unionAccesses(terminal, numColumns - numOutputs, node.out, 0, numOutputs); // NOSONAR node cannot be null here
            }
            case COLSELECT -> {
                // apply selection to predecessor's outCols
                // (there is exactly one predecessor)
                final int[] selection = getColumnSelection(spec);
                unionAccesses(terminal, predecessorTerminal, numColumns, i -> selection[i]);
            }
        }

        // control flow:
        switch (type) {
            case SOURCE, SLICE, ROWINDEX, APPEND, CONCATENATE, OBSERVER -> {
                // link to the new node.
                //
                // NB: Everything link from the predecessorTerminal has already
                //     been re-linked from the new node by the constructor.
                terminal.linkTo(node); // NOSONAR node cannot be null here
            }
            case MAP, APPENDMAP, APPENDMISSING, COLSELECT -> {
                // link to everything that was linked to by predecessor
                // (there is exactly one predecessor)
                //
                // NB: Nodes of these spec types are not themselves linked with
                //     control flow edges. Therefore, no re-linking of edges
                //     from predecessorTerminal has happened in the Node
                //     constructor.
                predecessorTerminal.forEachControlFlowEdge(e -> e.relinkFrom(terminal)); // NOSONAR predecessorTerminal cannot be null here
            }
            case ROWFILTER -> { // NOSONAR
                // link to other ROWFILTERs that were linked to by predecessor
                // (there is exactly one predecessor)
                //
                // NB: The controlFlowEdges().isEmpty() check is necessary:
                //     At this point, we have constructed a new ROWFILTER node.
                //     If the predecessorTerminal did already link to one or
                //     more other ROWFILTERs, these links are still connected to
                //     predecessorTerminal and we need to re-link them. However,
                //     if the predecessorTerminal linked to some other node
                //     type, that link has already been re-linked from the bew
                //     node by the constructor.
                if (!predecessorTerminal.controlFlowEdges().isEmpty() // NOSONAR predecessorTerminal cannot be null here
                    && predecessorTerminal.controlFlowTarget(0).type == ROWFILTER) {
                    predecessorTerminal.forEachControlFlowEdge(e -> e.relinkFrom(terminal));
                }
                // link to the new node
                terminal.linkTo(node); // NOSONAR node cannot be null here
            }
        }
    }

    /**
     * Control-flow and accesses at the root (output, CONSUMER, sink, ...) of this {@code TableTransformGraph}.
     */
    public Port terminal() {
        return terminal;
    }

    /**
     * number of columns at the root (output, CONSUMER, sink, ...) of this {@code TableTransformGraph}.
     */
    public int numColumns() {
        return terminal.accesses().size();
    }

    /**
     * Returns the number of rows of this {@code TableTransformGraph}, or a
     * negative value if the number of rows cannot be determined.
     *
     * @return number of rows in this {@code TableTransformGraph}
     */
    public long numRows() {
        return TableTransformGraphProperties.numRows(terminal);
    }

    /**
     * Returns the {@link CursorType} supported by this {@code
     * TableTransformGraph} (without additional prefetching and buffering). The
     * result is determined by the {@code CursorType} of the sources, and the
     * presence of ROWFILTER operations, etc.
     *
     * @return cursor supported by this {@code TableTransformGraph}
     */
    public CursorType supportedCursorType() {
        return TableTransformGraphProperties.supportedCursorType(this);
    }

    /**
     * Returns the {@code ColumnarSchema} at the {@link #terminal()} of this
     * {@code TableTransformGraph}.
     *
     * @return schema of this {@code TableTransformGraph}
     */
    public ColumnarSchema createSchema() {
        return TableTransformGraphProperties.schema(this);
    }

    @Override
    public String toString() {
        return "TableTransformGraph" + DependencyGraph.prettyPrint(this);
    }

    /**
     * Create a new {@code TableTransformGraph} by appending the given {@code
     * spec} to (a {@link #copy} of) this graph.
     *
     * @param spec TableTransformSpec to append
     * @return a copy of this graph with the new spec appended.
     */
    public TableTransformGraph append(final TableTransformSpec spec) {
        return new TableTransformGraph(spec, List.of(copy()));
    }

    /**
     * Make an independent copy of this {@code TableTransformGraph}.
     */
    public TableTransformGraph copy() {
        return new TableTransformGraph(new Copier().copyInPort(terminal, null));
    }

    private static class Copier {
        private final Map<Node, Node> nodes = new HashMap<>();

        private final Map<AccessId, AccessId> accessIds = new HashMap<>();

        private Port copyInPort(final Port port, final Node owner) {
            final Port portCopy = new Port(owner);
            port.accesses().forEach(a -> portCopy.accesses().add(copyOf(a.find())));
            port.controlFlowEdges().forEach(e -> portCopy.linkTo(copyOf(e.to().owner())));
            return portCopy;
        }

        private Node copyOf(final Node node) {
            final Node n = nodes.get(node);
            if (n != null) {
                return n;
            }
            final Node nodeCopy = new Node(node.getTransformSpec());
            nodes.put(node, nodeCopy);
            node.out.accesses().forEach(a -> nodeCopy.out.accesses().add(copyOf(a.find())));
            node.in.forEach(port -> nodeCopy.in.add(copyInPort(port, nodeCopy)));
            return nodeCopy;
        }

        private AccessId copyOf(final AccessId access) {
            final AccessId a = accessIds.get(access);
            if (a != null) {
                return a;
            }
            final AccessId.Producer producerCopy = copyOf(access.producer());
            return accessIds.computeIfAbsent(access, ac -> new AccessId(producerCopy, access.label()));
        }

        private AccessId.Producer copyOf(final AccessId.Producer producer) {
            return new AccessId.Producer(copyOf(producer.node()), producer.index());
        }
    }

    private static void unionAccesses(final Port from, final Port to, final int n) {
        unionAccesses(from, 0, to, 0, n);
    }

    private static void unionAccesses(final Port from, final int fromStartPos, final Port to, final int toStartPos,
        final int n) {
        for (int i = 0; i < n; i++) {
            from.access(i + fromStartPos).union(to.access(i + toStartPos));
        }
    }

    private static void unionAccesses(final Port from, final Port to, final int n, final IntUnaryOperator indexMapper) {
        for (int i = 0; i < n; i++) {
            from.access(i).union(to.access(indexMapper.applyAsInt(i)));
        }
    }

    private static int[] getColumnSelection(final TableTransformSpec spec) {
        return switch (SpecType.forSpec(spec)) {
            case MAP -> ((MapTransformSpec)spec).getColumnSelection();
            case ROWFILTER -> ((RowFilterTransformSpec)spec).getColumnSelection();
            case COLSELECT -> ((SelectColumnsTransformSpec)spec).getColumnSelection();
            case OBSERVER -> ((ObserverTransformSpec)spec).getColumnSelection();
            default -> throw new IllegalArgumentException();
        };
    }

    /**
     * Create {@code n} new {@code AccessId}s with the given {@code
     * producerNode} (may be {@code null}). The {@code AccessId}s will be
     * labeled by applying the given {@code label} function to {@code 0, 1, ...,
     * n-1}.
     *
     * @param producerNode
     * @param n
     * @param label
     * @return
     */
    static List<AccessId> createAccessIds(final Node producerNode, final int n,
            final IntFunction<String> label) {
        final List<AccessId> cols = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            AccessId.Producer producer = producerNode == null ? null : new AccessId.Producer(producerNode, i);
            cols.add(new AccessId(producer, label.apply(i)));
        }
        return cols;
    }

    /**
     * Create an AccessId labeling function to produce labels like
     * "alpha^i_v1(0)" (this would be created for {@code varName="alpha"},
     * {@code nodeId=1}, {@code predecessorIndex=0}).
     */
    static IntFunction<String> accessLabel(final String varName, final int nodeId, final int predecessorIndex) {
        return i -> {
            String label = varName + "^" + i + "_v" + nodeId;
            if (predecessorIndex >= 0) {
                label += "(" + predecessorIndex + ")";
            }
            return label;
        };
    }
}
