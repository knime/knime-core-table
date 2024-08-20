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
package org.knime.core.table.virtual.graph.rag2;

import static org.knime.core.table.virtual.graph.rag.RagEdgeType.SPEC;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.knime.core.table.virtual.TableTransform;
import org.knime.core.table.virtual.VirtualTable;
import org.knime.core.table.virtual.graph.rag.ConsumerTransformSpec;
import org.knime.core.table.virtual.graph.rag.RagEdgeType;
import org.knime.core.table.virtual.graph.rag.RagNodeType;
import org.knime.core.table.virtual.spec.MapTransformSpec;
import org.knime.core.table.virtual.spec.RowFilterTransformSpec;
import org.knime.core.table.virtual.spec.SelectColumnsTransformSpec;
import org.knime.core.table.virtual.spec.SourceTransformSpec;
import org.knime.core.table.virtual.spec.TableTransformSpec;

public class SpecGraphBuilder {


    static List<AccessId> createCols(final int n, final String debugVarName, int debugNodeId, int debugPredecessorIndex) {
        return createCols(null, n, debugVarName, debugNodeId, debugPredecessorIndex);
    }

    static List<AccessId> createCols(final Node producer, final int n, final String debugVarName, int debugNodeId, int debugPredecessorIndex) {
        final List<AccessId> cols = new ArrayList<>( n );
        for (int i = 0; i < n; i++) {
            String label = debugVarName + "^" + i + "_v" + debugNodeId;
            if (debugPredecessorIndex >= 0)
                label += "(" + debugPredecessorIndex + ")";
            cols.add(new AccessId(producer, producer != null ? i : -1, label));
        }
        return cols;
    }

    record ControlFlowEdge(PredecessorData from, Node to) {}

    record PredecessorData(
            Node node,
            Node predecessor,
            List<AccessId> inCols,
            List<AccessId> inputs,
            List<ControlFlowEdge> controlFlowEdges) {
        PredecessorData(Node node, Node predecessor, List<AccessId> inCols, List<AccessId> inputs) {
            this(node, predecessor, inCols, inputs, new ArrayList<>());
        }
    }

    static Iterable<Node> specPredecessors(Node first) {
        return () -> new Iterator<>() {
            private Node next = first;

            @Override
            public boolean hasNext() {
                return next != null;
            }

            @Override
            public Node next() {
                final Node current = next;
                next = next.specPredecessor();
                return current;
            }
        };
    }


    static class Node {

        final RagNodeType type;

        final TableTransformSpec spec;

        final int numColumns;

        final List<AccessId> outCols;

        final List<AccessId> outputs;

        final List<ControlFlowEdge> controlFlowEdges = new ArrayList<>();

        final List<PredecessorData> predecessorData;

        public Node(
                final TableTransformSpec spec,
                final List<Node> predecessors ) {
            this.spec = spec;
            this.type = RagNodeType.forSpec(spec);

            predecessorData = new ArrayList<>();
            for (int i = 0; i < predecessors.size(); i++) {
                final Node p = predecessors.get(i);
                final List<AccessId> inCols = createCols(p.numColumns, "alpha", id, predecessors.size() > 1 ? i : -1);

                final int numInputs = switch (type) {
                    case SOURCE, COLFILTER, SLICE, ROWINDEX  -> 0;
                    case MAP -> ((MapTransformSpec)spec).getColumnSelection().length;
                    case ROWFILTER -> ((RowFilterTransformSpec)spec).getColumnSelection().length;
                    case APPEND, CONCATENATE, CONSUMER -> p.numColumns;
                    case APPENDMISSING, MISSING, MATERIALIZE, WRAPPER, IDENTITY, OBSERVER -> throw unhandledNodeType();
                };
                final List<AccessId> inputs = createCols(numInputs, "gamma", id, predecessors.size() > 1 ? i : -1);
                final PredecessorData pd = new PredecessorData(this, p, inCols, inputs);
                final List<ControlFlowEdge> inflow = pd.controlFlowEdges();
                switch (type) {
                    case MAP, COLFILTER, SOURCE -> {
                    }
                    case SLICE, APPEND, CONCATENATE, ROWINDEX, CONSUMER -> {
                        boolean rowFilterHit = false;
                        for (Node node : specPredecessors(p)) {
                            switch (node.type) {
                                case MAP, COLFILTER -> {
                                    continue;
                                }
                                case ROWFILTER -> {
                                    rowFilterHit = true;
                                    inflow.add(new ControlFlowEdge(pd, node));
                                    continue;
                                }
                                default -> {
                                    if (!rowFilterHit) {
                                        inflow.add(new ControlFlowEdge(pd, node));
                                    }
                                }
                                case APPENDMISSING, MISSING, MATERIALIZE, WRAPPER, IDENTITY, OBSERVER -> throw unhandledNodeType();
                            }
                            break;
                        }
                    }
                    case ROWFILTER -> {
                        for (Node node : specPredecessors(p)) {
                            switch (node.type) {
                                case MAP, COLFILTER, ROWFILTER -> {
                                    continue;
                                }
                                default -> {
                                    inflow.add(new ControlFlowEdge(pd, node));
                                }
                                case APPENDMISSING, MISSING, MATERIALIZE, WRAPPER, IDENTITY, OBSERVER -> throw unhandledNodeType();
                            }
                            break;
                        }
                    }
                    case APPENDMISSING, MISSING, MATERIALIZE, WRAPPER, IDENTITY, OBSERVER -> throw unhandledNodeType();
                }
                inflow.forEach(edge -> {
                    System.out.println("control flow: " + edge.from.node.id + " --> " + edge.to.id);
                });
                predecessorData.add(pd);
            }

            numColumns = switch (type) {
                case SOURCE -> ((SourceTransformSpec)spec).getSchema().numColumns();
                case MAP -> ((MapTransformSpec)spec).getMapperFactory().getOutputSchema().numColumns();
                case ROWINDEX -> predecessors.get(0).numColumns + 1;
                case COLFILTER -> ((SelectColumnsTransformSpec)spec).getColumnSelection().length;
                case APPEND -> predecessors.stream().mapToInt(n -> n.numColumns).sum();
                case SLICE, ROWFILTER, CONCATENATE -> predecessors.get(0).numColumns;
                case CONSUMER -> 0; // TODO (TP) or should it be predecessors.get(0).numColumns ???
                case APPENDMISSING, MISSING, MATERIALIZE, WRAPPER, IDENTITY, OBSERVER ->
                        throw new UnsupportedOperationException(
                                "not handled yet. needs to be implemented or removed"); // TODO (TP)
            };
            outCols = createCols(numColumns, "beta", id, -1);

            final int numOutputs = switch (type) {
                case SOURCE, MAP, APPEND, CONCATENATE, CONSUMER -> numColumns;
                case ROWINDEX -> 1;
                case SLICE, COLFILTER, ROWFILTER -> 0;
                case APPENDMISSING, MISSING, MATERIALIZE, WRAPPER, IDENTITY, OBSERVER ->
                        throw new UnsupportedOperationException(
                                "not handled yet. needs to be implemented or removed"); // TODO (TP)
            };
            outputs = createCols( this, numOutputs, "delta", id, -1);

            // Access tracing
            traceIncolsToOutcols();
            tracePredecessorIn();
            traceOutcolsToOutputs();
            traceInputsToIncols();
        }

        Node specPredecessor() {
            return predecessorData.isEmpty() ? null : predecessorData.get(0).predecessor;
        }

        AccessId outCol(int i) {
            return outCols.get(i);
        }

        AccessId inCol(int i) {
            return inCol(0, i);
        }

        AccessId inCol(int p, int i) {
            return predecessorData.get(p).inCols.get(i);
        }

        AccessId output(int i) {
            return outputs.get(i);
        }

        AccessId input(int i) {
            return input(0, i);
        }

        AccessId input(int p, int i) {
            return predecessorData.get(p).inputs.get(i);
        }

        /**
         * link predecessor outcols to incols of this node
         */
        private void tracePredecessorIn() {
            for (int j = 0; j < predecessorData.size(); ++j) {
                final Node p = predecessorData.get(j).predecessor();
                for (int i = 0; i < p.numColumns; i++) {
                    inCol(j, i).union(p.outCol(i));
                }
            }
        }

        /**
         * link incols to outcols for pass-through columns
         */
        private void traceIncolsToOutcols()
        {
            switch (type) {
                case SOURCE, MAP, APPEND, CONCATENATE, CONSUMER -> {
                    // do nothing
                }
                case SLICE, ROWFILTER -> {
                    // pass-through
                    var inCols = predecessorData.get(0).inCols;
                    for (int i = 0; i < numColumns; i++) {
                        outCol(i).union(inCol(i));
                    }
                }
                case ROWINDEX -> {
                    // pass-through except last outcol
                    var inCols = predecessorData.get(0).inCols;
                    for (int i = 0; i < numColumns - 1; i++) {
                        outCol(i).union(inCol(i));
                    }
                }
                case COLFILTER -> {
                    // apply selection
                    final int[] selection = ((SelectColumnsTransformSpec)spec).getColumnSelection();
                    for (int i = 0; i < selection.length; i++) {
                        outCol(i).union(inCol(selection[i]));
                    }
                }
                case APPENDMISSING, MISSING, MATERIALIZE, WRAPPER, IDENTITY, OBSERVER -> throw unhandledNodeType();
            }
        }

        /**
         * link outcols to outputs (columns that are produced by this node)
         */
        private void traceOutcolsToOutputs()
        {
            switch (type) {
                case SOURCE, MAP, APPEND, CONCATENATE, CONSUMER -> {
                    for (int i = 0; i < numColumns; i++) {
                        outCol(i).union(output(i));
                    }
                }
                case ROWINDEX -> {
                    outCol(numColumns - 1).union(output(0));
                }
                case SLICE, COLFILTER, ROWFILTER -> {
                }
                case APPENDMISSING, MISSING, MATERIALIZE, WRAPPER, IDENTITY, OBSERVER -> throw unhandledNodeType();
            }
        }

        /**
         * link incols to inputs (columns that are used by this node)
         */
        private void traceInputsToIncols()
        {
            switch (type) {
                case MAP -> {
                    final int[] selection = ((MapTransformSpec)spec).getColumnSelection();
                    for (int i = 0; i < selection.length; i++) {
                        input(i).union(inCol(selection[i]));
                    }
                }
                case ROWFILTER -> {
                    final int[] selection = ((RowFilterTransformSpec)spec).getColumnSelection();
                    for (int i = 0; i < selection.length; i++) {
                        input(i).union(inCol(selection[i]));
                    }
                }
                case APPEND, CONCATENATE, CONSUMER -> {
                    for (int j = 0; j < predecessorData.size(); ++j) {
                        final Node p = predecessorData.get(j).predecessor(); // TODO: Add convenience method predecessor(j) --> predecessorData.get(j).predecessor()
                        for (int i = 0; i < p.numColumns; i++) {
                            input(j, i).union(inCol(j, i));
                        }
                    }
                }
                case SOURCE, SLICE, COLFILTER, ROWINDEX -> {
                }
                case APPENDMISSING, MISSING, MATERIALIZE, WRAPPER, IDENTITY, OBSERVER -> throw unhandledNodeType();
            }
        }

        @Override
        public String toString() {
            return toString("");
        }

        private String toString(String indent) {
            final StringBuilder sb = new StringBuilder(indent + "Node{");
            sb.append("id=").append(id);
            sb.append(", type=").append(type);
            sb.append(", spec=").append(spec);
            sb.append(", numColumns=").append(numColumns);

//            final List<AccessId> inCols = predecessorData.stream().flatMap(pd -> pd.inCols.stream()).toList();
//            sb.append(", inCols=").append(inCols);
//            sb.append(", outCols=").append(outCols);

            final List<AccessId> inputs = predecessorData.stream().flatMap(pd -> pd.inputs.stream()).toList();
            sb.append(", inputs").append(inputs);
            sb.append(", outputs=").append(outputs);

            final List<Node> predecessors = predecessorData.stream().map(pd -> pd.predecessor).toList();
            if (predecessors.isEmpty()) {
                sb.append(", predecessors={}");
            } else {
                sb.append(", predecessors={\n");
                for (Node predecessor : predecessors) {
                    sb.append(predecessor.toString(indent + "  "));
                    sb.append(",\n");
                }
                sb.append(indent + "}");
            }
            sb.append('}');
            return sb.toString();
        }


        // ids are just for printing ...
        private static int nextNodeId = 0;
        private final int id = nextNodeId++;

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



    public static class SpecGraph {
        final List<Node> nodes = new ArrayList<>();


        final Node root;

        public SpecGraph(Node root) {
            this.root = root;
            addRecursively(root);
        }

        private void addRecursively(Node node) {
            nodes.add(node);
            node.predecessorData.forEach(d -> addRecursively(d.predecessor));
        }

        @Override
        public String toString() {
            return "SpecGraph{root=\n" + root.toString("  ") + "\n}";
        }
    }


    record Edge(RagEdgeType type, Node source, Node target) {
    }

    static class Edges {
        final Set<Edge> edges = new HashSet<>();

        Edges(Node root) {
            addRecursively1(root);
            addRecursively2(root);
        }

        private void addRecursively1(Node node) {
            for (PredecessorData pd : node.predecessorData) {
                edges.add(new Edge(RagEdgeType.SPEC, node, pd.predecessor));
                addRecursively1(pd.predecessor);
            }
        }

        private void addRecursively2(Node node) {
            for (PredecessorData pd : node.predecessorData) {
                pd.controlFlowEdges.forEach(cfe -> edges.add( //
                        new Edge(RagEdgeType.EXEC, node, cfe.to())));

                pd.inputs.forEach(accessId -> edges.add( //
                        new Edge(RagEdgeType.DATA, node, accessId.find().producer)));

                addRecursively2(pd.predecessor);
            }
        }
    }

    public static String mermaid(final SpecGraph graph, final boolean darkMode) {
        final var sb = new StringBuilder("graph TD\n");
        for (final Node node : graph.nodes) {
            final String name = "<" + node.id() + "> " + node.spec.toString();
            sb.append("  " + node.id() + "(\"" + name + "\")\n");
        }
        int edgeId = 0;
        final Set<Edge> edges = new Edges(graph.root).edges;
        for (final Edge edge : edges) {
            if (edge.type() == SPEC) {
                sb.append("  " + edge.target().id() + "--- " + edge.source().id() + "\n");
            } else {
                sb.append("  " + edge.target().id() + "--> " + edge.source().id() + "\n");
            }
            switch (edge.type() )
            {
                case SPEC:
                    sb.append("  linkStyle " + edgeId + " stroke:" + //
                            (darkMode ? "#444444,anything" : "#DDDDDD,anything") + ";\n");
                    break;
                case DATA:
                    sb.append("  linkStyle " + edgeId + " stroke:" + //
                            (darkMode ? "blue" : "#8888FF,anything") + ";\n");
                    break;
                case EXEC:
                    sb.append("  linkStyle " + edgeId + " stroke:" + //
                            (darkMode ? "red" : "#FF8888,anything") + ";\n");
                    break;
                case ORDER:
                    sb.append("  linkStyle " + edgeId + " stroke:" + //
                            (darkMode ? "white" : "black") + ";\n");
                    break;
                case FLATTENED_ORDER:
                    sb.append("  linkStyle " + edgeId + " stroke:" + //
                            (darkMode ? "lime" : "lime") + ";\n");
                    break;
            }
            ++edgeId;
        }
        return sb.toString();
    }

    static class AccessId {
        final Node producer;

        final int outColIndex;

        AccessId parent;

        final String label; // TODO (for debugging only)

        public AccessId(final Node producer, final int outColIndex, String label) {
            this.producer = producer;
            this.outColIndex = outColIndex;
            this.parent = this;
            this.label = label;
        }

        public AccessId(String label) {
            this(null, -1, label);
        }

        public AccessId find() {
            if (parent == this) {
                return this;
            } else {
                final AccessId p = parent.find();
                if (parent != p) {
                    parent = p;
                }
                return p;
            }
        }

        public void union(AccessId other) {
            parent = other.find();
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            //            sb.append("{");
            sb.append(label);

            if (producer != null) {
                sb.append("(producer=").append(producer.id);
                sb.append(", ").append(outColIndex);
                sb.append(")");
            }

            final AccessId setLeader = find();
            if (setLeader != this) {
                sb.append("-->").append(setLeader);
            }
            //            sb.append('}');
            return sb.toString();
        }
    }



    /**
     * Builds a spec graph from {@code table}.
     *
     * @param table the table
     * @return spec graph representing the given table
     */
    public static SpecGraph buildSpecGraph(final VirtualTable table) {
        return buildSpecGraph(table.getProducingTransform());
    }

    /**
     * Builds a spec graph from {@code tableTransform}.
     *
     * @param tableTransform the producingTransform of the table
     * @return spec graph representing the given table
     */
    public static SpecGraph buildSpecGraph(final TableTransform tableTransform) {
        final var consumerTransform = new TableTransform(//
                List.of(tableTransform),//
                new ConsumerTransformSpec());
        final Node root = createNodes(consumerTransform);
        return new SpecGraph(root);
    }

    /**
     * Create a Node for the given TableTransform, and recursively create Nodes for its precedingTransforms.
     *
     * @param transform the TableTransform
     * @return the Node corresponding to {@code transform}
     */
    // TODO: make iterative instead of recursive
    private static Node createNodes(final TableTransform transform) {
        final TableTransformSpec spec = transform.getSpec();
        final List<Node> predecessors = transform.getPrecedingTransforms().stream().map(SpecGraphBuilder::createNodes).toList();
        return new Node(spec, predecessors);
    }

    private static UnsupportedOperationException unhandledNodeType() {
        return new UnsupportedOperationException("not handled yet. needs to be implemented or removed");
    }

}








/*
                switch (type) {
                    case SOURCE -> {
                    }
                    case SLICE -> {
                    }
                    case APPEND -> {
                    }
                    case CONCATENATE -> {
                    }
                    case COLFILTER -> {
                    }
                    case MAP -> {
                    }
                    case ROWFILTER -> {
                    }
                    case CONSUMER -> {
                    }
                    case ROWINDEX -> {
                    }
                    case APPENDMISSING, MISSING, MATERIALIZE, WRAPPER, IDENTITY, OBSERVER ->
                            throw new UnsupportedOperationException(
                                    "not handled yet. needs to be implemented or removed"); // TODO (TP)
                }

 */