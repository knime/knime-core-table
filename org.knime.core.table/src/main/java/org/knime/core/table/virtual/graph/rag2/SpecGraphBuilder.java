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

import java.util.ArrayList;
import java.util.List;

import org.knime.core.table.virtual.TableTransform;
import org.knime.core.table.virtual.VirtualTable;
import org.knime.core.table.virtual.graph.rag.ConsumerTransformSpec;
import org.knime.core.table.virtual.graph.rag.RagNodeType;
import org.knime.core.table.virtual.spec.MapTransformSpec;
import org.knime.core.table.virtual.spec.SelectColumnsTransformSpec;
import org.knime.core.table.virtual.spec.SourceTransformSpec;
import org.knime.core.table.virtual.spec.TableTransformSpec;

public class SpecGraphBuilder {


    static List<AccessId> createCols(final int n, final String debugVarName, int debugNodeId, int debugPredecessorIndex) {
        final List<AccessId> inCols = new ArrayList<>( n );
        for (int i = 0; i < n; i++) {
            String label = debugVarName + "^" + i + "_v" + debugNodeId;
            if (debugPredecessorIndex >= 0)
                label += "(" + debugPredecessorIndex + ")";
            inCols.add(new AccessId(label));
        }
        return inCols;
    }


    static class Node {
        private static int ids = 0;

        final int id = ids++;

        final RagNodeType type;

        final TableTransformSpec spec;

        final int numColumns;

        final List<AccessId> outCols;

        record PredecessorData(Node predecessor, List<AccessId> inCols) {
        }

        final List<PredecessorData> predecessorData;

        public Node(
                final RagNodeType type,
                final TableTransformSpec spec,
                final int numColumns,
                final List<Node> predecessors ) {
            this.type = type;
            this.spec = spec;
            this.numColumns = numColumns;
            predecessorData = new ArrayList<>();
            for (int i = 0; i < predecessors.size(); i++) {
                final Node p = predecessors.get(i);
                final List<AccessId> inCols = createCols(p.numColumns, "alpha", id, predecessors.size() > 1 ? i : -1);
                predecessorData.add( new PredecessorData(p, inCols));
            }

            outCols = createCols(numColumns, "beta", id, -1);
            traceInOut();
            tracePredecessorIn();
        }

        AccessId outCol( int i ) {
            return outCols.get(i);
        }

        AccessId inCol( int i ) {
            return inCol(0, i);
        }

        AccessId inCol( int p, int i ) {
            return predecessorData.get(p).inCols.get(i);
        }

        private void tracePredecessorIn() {
            for (int j = 0; j < predecessorData.size(); ++j) {
                final Node p = predecessorData.get(j).predecessor();
                for (int i = 0; i < p.numColumns; i++) {
                    inCol(j, i).union(p.outCol(i));
                }
            }
        }

        private void traceInOut()
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
                case APPENDMISSING, MISSING, MATERIALIZE, WRAPPER, IDENTITY, OBSERVER ->
                        throw new UnsupportedOperationException(
                                "not handled yet. needs to be implemented or removed"); // TODO (TP)
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

            final List<AccessId> inCols = predecessorData.stream().flatMap(pd -> pd.inCols.stream()).toList();
            sb.append(", inCols=").append(inCols);

            sb.append(", outCols=").append(outCols);

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

    }

    static class SpecGraph {
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

    static class AccessId
    {
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

            if ( producer != null ) {
                sb.append("(producer=").append(producer.id);
                sb.append(", ").append(outColIndex);
                sb.append(")");
            }

            final AccessId setLeader = find();
            if ( setLeader != this ) {
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
        final RagNodeType type = RagNodeType.forSpec(spec);
        final int numColumns = switch (type) {
            case SOURCE -> ((SourceTransformSpec)spec).getSchema().numColumns();
            case MAP -> ((MapTransformSpec)spec).getMapperFactory().getOutputSchema().numColumns();
            case ROWINDEX -> predecessors.get(0).numColumns + 1;
            case COLFILTER -> ((SelectColumnsTransformSpec)spec).getColumnSelection().length;
            case APPEND -> predecessors.stream().mapToInt(n -> n.numColumns).sum();
            case SLICE, ROWFILTER, CONCATENATE -> predecessors.get(0).numColumns;
            case CONSUMER -> 0;
            case APPENDMISSING, MISSING, MATERIALIZE, WRAPPER, IDENTITY, OBSERVER ->
                    throw new UnsupportedOperationException(
                            "not handled yet. needs to be implemented or removed"); // TODO (TP)
        };
        return new Node(type, spec, numColumns, predecessors);
    }
}
