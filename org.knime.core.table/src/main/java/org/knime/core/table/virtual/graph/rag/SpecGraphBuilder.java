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

import static org.knime.core.table.virtual.graph.rag.RagEdgeType.SPEC;
import static org.knime.core.table.virtual.graph.rag.RagGraphUtils.topologicalSort;
import static org.knime.core.table.virtual.graph.rag.RagNodeType.CONCATENATE;
import static org.knime.core.table.virtual.graph.rag.RagNodeType.CONSUMER;
import static org.knime.core.table.virtual.graph.rag.RagNodeType.MAP;
import static org.knime.core.table.virtual.graph.rag.RagNodeType.OBSERVER;
import static org.knime.core.table.virtual.graph.rag.RagNodeType.SLICE;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.knime.core.table.row.Selection;
import org.knime.core.table.virtual.TableTransform;
import org.knime.core.table.virtual.VirtualTable;
import org.knime.core.table.virtual.spec.AppendMissingValuesTransformSpec;
import org.knime.core.table.virtual.spec.MapTransformSpec;
import org.knime.core.table.virtual.spec.MaterializeTransformSpec;
import org.knime.core.table.virtual.spec.ObserverTransformSpec;
import org.knime.core.table.virtual.spec.SelectColumnsTransformSpec;
import org.knime.core.table.virtual.spec.SliceTransformSpec;
import org.knime.core.table.virtual.spec.SourceTransformSpec;
import org.knime.core.table.virtual.spec.TableTransformSpec;

/**
 * Static methods for building spec graphs from {@link VirtualTable}s.
 * <p>
 * A <em>spec graph</em> is the initial {@link RagGraph} reflecting a {@code
 * VirtualTable}. It contains a node for each {@code TableTransform} in the
 * {@code VirtualTable}, and SPEC edges between them (pointing from producing to
 * consuming {@code TableTransform}). Fork-join-like structures are resolved
 * during construction, and {@link RagNode#numColumns()} is computed for every
 * node. No other processing is done.
 * <p>
 * Subsequently, the spec graph can be passed to {@link
 * RagBuilder#createOrderedRag(RagGraph)} for optimization.
 */
public class SpecGraphBuilder {

    /**
     * Builds a spec graph from {@code table}.
     *
     * @param table the table
     * @return spec graph representing the given table
     */
    public static RagGraph buildSpecGraph(final VirtualTable table) {
        return buildSpecGraph(table.getProducingTransform());
    }

    /**
     * Builds a spec graph from {@code tableTransform}.
     *
     * @param tableTransform the producingTransform of the table
     * @return spec graph representing the given table
     */
    public static RagGraph buildSpecGraph(final TableTransform tableTransform) {

        SpecGraphBuilder builder = new SpecGraphBuilder();
        builder.buildSpec(tableTransform);
        return builder.graph;
    }

    /**
     * Spawning nodes create new sub-trees of their SPEC predecessor nodes. This
     * prevents (incorrect) fork-join type fusion of branches when forwarding structure
     * diverges.
     */
    private static final EnumSet<RagNodeType> spawningNodeTypes = EnumSet.of(SLICE, CONCATENATE);

    final RagGraph graph = new RagGraph();

    SpecGraphBuilder() {
    }

    /**
     * Build RagGraph nodes for each TableTransform in the given VirtualTable, and SPEC
     * edges between them. Set numColumns for each node.
     */
    void buildSpec(final TableTransform tableTransform) {

        // For now, we expect only a single output table.
        // Either the final transform is already a MaterializeTransformSpec, in
        // which case this becomes the root node (a MATERIALIZE node).
        // Or, the final transform just describes some other VirtualTable, in which
        // case, an artificial CONSUMER node is appended and becomes the root.
        if (tableTransform.getSpec() instanceof MaterializeTransformSpec) {
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
     * @param transform the TableTransform
     * @param nodeLookup Links TableTransforms to already existing Nodes. This is used to handle
     *                   fork-join-type structures.
     * @return the Node corresponding to {@code transform}
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
                Map<TableTransform, RagNode> lookup =
                        spawningNodeTypes.contains(node.type()) ? new HashMap<>() : nodeLookup;
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
        topologicalSort(graph, SPEC).forEach(SpecGraphBuilder::buildNumColumns);
    }

    private static void buildNumColumns(final RagNode node) {
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
            case OBSERVER:
                numColumns = node.predecessor(SPEC).numColumns();
                break;
            case ROWINDEX:
                // append one column for the row index
                numColumns = node.predecessor(SPEC).numColumns() + 1;
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
                throw new IllegalStateException("Unexpected value: " + node.type());
        }
        node.setNumColumns(numColumns);
    }

    /**
     * Creates a copy of {@code specGraph}, with SLICE and COLFILTER operations
     * appended that implement the given {@code selection}.
     * <p>
     * The number of columns of the root node is recomputed.
     *
     * @param specGraph a spec graph
     * @param selection the selection (columns and row range) to append
     * @return copy spec graph with appended selection
     */
    public static RagGraph appendSelection(final RagGraph specGraph, final Selection selection) {

        RagGraph graph = specGraph.copy();

        final RagNode root = graph.getRoot();
        if (root.type() != CONSUMER)
            throw new IllegalArgumentException();

        if (!selection.rows().allSelected()) {
            final TableTransform sliceTransform = new TableTransform(//
                    Collections.emptyList(),//
                    new SliceTransformSpec(selection.rows()));
            final RagNode slice = graph.addNode(sliceTransform);
            graph.relinkPredecessorsToNewTarget(root, slice, SPEC);
            graph.addEdge(slice, root, SPEC);
            buildNumColumns(slice);
        }

        if (!selection.columns().allSelected()) {
            final TableTransform selectColsTransform = new TableTransform(//
                    Collections.emptyList(),//
                    new SelectColumnsTransformSpec(selection.columns().getSelected()));
            final RagNode selectCols = graph.addNode(selectColsTransform);
            graph.relinkPredecessorsToNewTarget(root, selectCols, SPEC);
            graph.addEdge(selectCols, root, SPEC);
            buildNumColumns(selectCols);
        }

        buildNumColumns(root);

        return graph;
    }
}
