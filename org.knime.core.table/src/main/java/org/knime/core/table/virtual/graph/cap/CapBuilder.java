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
package org.knime.core.table.virtual.graph.cap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.knime.core.table.row.Selection;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.graph.rag.AccessId;
import org.knime.core.table.virtual.graph.rag.BranchGraph;
import org.knime.core.table.virtual.graph.rag.TableTransformGraph;
import org.knime.core.table.virtual.graph.rag.TableTransformGraphProperties;
import org.knime.core.table.virtual.spec.MapTransformSpec;
import org.knime.core.table.virtual.spec.ObserverTransformSpec;
import org.knime.core.table.virtual.spec.RowFilterTransformSpec;
import org.knime.core.table.virtual.spec.RowIndexTransformSpec;
import org.knime.core.table.virtual.spec.SliceTransformSpec;
import org.knime.core.table.virtual.spec.SourceTableProperties;
import org.knime.core.table.virtual.spec.SourceTransformSpec;

public class CapBuilder {

    public static CursorAssemblyPlan createCursorAssemblyPlan(final BranchGraph sequentializedGraph) {
        final CapBuilder builder = new CapBuilder(sequentializedGraph);
        final SourceTableProperties.CursorType cursorType =
                sequentializedGraph.tableTransformGraph().supportedCursorType();
        final long numRows = sequentializedGraph.tableTransformGraph().numRows();
        return new CursorAssemblyPlan(builder.cap, cursorType, numRows, builder.sourceSchemas);
    }

    private final List<CapNode> cap;

    private final Map<TableTransformGraph.Node, CapNode> capNodes;

    private final Map<AccessId, CapAccessId> capAccessIds;

    private final Map<UUID, ColumnarSchema> sourceSchemas;

    private CapBuilder(final BranchGraph sequentializedGraph) {
        cap = new ArrayList<>();
        capNodes = new HashMap<>();
        capAccessIds = new HashMap<>();
        sourceSchemas = new HashMap<>();

        BranchGraph.BranchEdge branch = sequentializedGraph.rootBranch();

        final CapNode capNode = appendBranch(branch);
        final CapAccessId[] inputs = capAccessIdsFor(sequentializedGraph.tableTransformGraph().terminal().accesses());
        cap.add(new CapNodeConsumer(index++, inputs, capNode.index()));
    }

    private int index = 0;

    private CapNode appendBranch(final BranchGraph.BranchEdge branch) {
        // append predecessor branches
        final List<CapNode> heads = new ArrayList<>();
        branch.target().branches().forEach(b -> heads.add(appendBranch(b)));

        // current head while building this branch
        CapNode capNode = appendBranchTarget(branch.target(), heads);

        // append inner nodes
        for (BranchGraph.InnerNode innerNode : branch.innerNodes()) {
            capNode = appendInnerNode(innerNode, capNode);
        }

        return capNode;
    }

    private CapNode appendBranchTarget(final BranchGraph.BranchNode branchTarget, final List<CapNode> prededessors) {

        final TableTransformGraph.Node node = branchTarget.node();
        final int numPredecessors = prededessors.size();
        final int[] predecessorIndices = new int[numPredecessors];
        final long[] predecessorSizes = new long[numPredecessors];
        for (int i = 0; i < numPredecessors; ++i) {
            predecessorIndices[i] = prededessors.get(i).index();
            predecessorSizes[i] = TableTransformGraphProperties.numRows(node.in(i));
        }

        final List<AccessId> outputs = node.out().accesses();
        final CapNode capNode;
        switch (node.type()) {
            case SOURCE -> {
                final SourceTransformSpec spec = node.getTransformSpec();
                final UUID uuid = spec.getSourceIdentifier();
                final int[] columns = outputs.stream().mapToInt(a -> a.find().producer().index()).toArray();
                final Selection.RowRangeSelection range = spec.getRowRange();
                capNode = new CapNodeSource(index++, uuid, columns, range);
                sourceSchemas.put(uuid, spec.getSchema());
            }
            case APPEND -> {
                final int[][] predecessorOutputIndices = new int[numPredecessors][];
                final List<AccessId> inputs = new ArrayList<>();
                for (int i = 0; i < numPredecessors; ++i) {
                    final List<AccessId> branchInputs = node.in(i).accesses();
                    predecessorOutputIndices[i] = new int[branchInputs.size()];
                    Arrays.setAll(predecessorOutputIndices[i], j -> j + inputs.size());
                    inputs.addAll(branchInputs);
                }
                final CapAccessId[] capInputs = capAccessIdsFor(inputs);
                capNode = new CapNodeAppend(index++, capInputs, predecessorIndices, predecessorOutputIndices, predecessorSizes);
            }
            case CONCATENATE -> {
                final CapAccessId[][] capInputs = new CapAccessId[numPredecessors][];
                Arrays.setAll(capInputs, i -> capAccessIdsFor(node.in(i).accesses()));
                capNode = new CapNodeConcatenate(index++, capInputs, predecessorIndices, predecessorSizes);
            }
            default -> throw new IllegalStateException();
        }

        createCapAccessIdsFor(outputs, capNode);
        append(node, capNode);
        return capNode;
    }

    private CapNode appendInnerNode(final BranchGraph.InnerNode innerNode, final CapNode predecessor) {

        final TableTransformGraph.Node node = innerNode.node();
        final List<AccessId> inputs = node.in(0).accesses();
        final CapAccessId[] capInputs = capAccessIdsFor(inputs);

        final CapNode capNode;
        switch (node.type()) {
            case MAP -> {
                final MapTransformSpec spec = node.getTransformSpec();
                final List<AccessId> outputs = node.out().accesses();
                final int[] columns = outputs.stream().mapToInt(a -> a.find().producer().index()).toArray();
                capNode = new CapNodeMap(index++, capInputs, predecessor.index(), columns, spec.getMapperFactory());
                createCapAccessIdsFor(outputs, capNode);
            }
            case SLICE -> {
                final SliceTransformSpec spec = node.getTransformSpec();
                final long from = spec.getRowRangeSelection().fromIndex();
                final long to = spec.getRowRangeSelection().toIndex();
                capNode = new CapNodeSlice(index++, predecessor.index(), from, to);
            }
            case ROWFILTER -> {
                final RowFilterTransformSpec spec = node.getTransformSpec();
                capNode = new CapNodeRowFilter(index++, capInputs, predecessor.index(), spec.getFilterFactory());
            }
            case ROWINDEX -> {
                final RowIndexTransformSpec spec = node.getTransformSpec();
                final List<AccessId> outputs = node.out().accesses();
                capNode = new CapNodeRowIndex(index++, predecessor.index(), spec.getOffset());
                createCapAccessIdsFor(outputs, capNode);
            }
            case OBSERVER -> {
                final ObserverTransformSpec spec = node.getTransformSpec();
                capNode = new CapNodeObserver(index++, capInputs, predecessor.index(), spec.getObserverFactory());
            }
            default -> throw new IllegalStateException();
        }

        append(node, capNode);
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
