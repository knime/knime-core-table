package org.knime.core.table.virtual.graph.rag;

import static org.knime.core.table.virtual.graph.rag.RagEdgeType.SPEC;
import static org.knime.core.table.virtual.graph.rag.RagNodeType.CONSUMER;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.knime.core.table.row.Selection;
import org.knime.core.table.virtual.TableTransform;
import org.knime.core.table.virtual.spec.SelectColumnsTransformSpec;
import org.knime.core.table.virtual.spec.SliceTransformSpec;

public class RagGraphSelection {

    // TODO (TP): move to SpecGraphBuilder
    public static void appendSelection(final RagGraph graph, final Selection selection) {

        final RagNode root = graph.getRoot();
        if (root.type() != CONSUMER)
            throw new IllegalArgumentException();

        if (!selection.rows().allSelected()) {
            final TableTransform sliceTransform = new TableTransform(//
                    Collections.emptyList(),//
                    new SliceTransformSpec(selection.rows()));
            final RagNode slice = graph.addNode(sliceTransform);
            relinkPredecessorsToNewTarget(graph, root, slice, SPEC);
            graph.addEdge(slice, root, SPEC);
            SpecGraphBuilder.buildNumColumns(slice);
        }

        if (!selection.columns().allSelected()) {
            final TableTransform selectColsTransform = new TableTransform(//
                    Collections.emptyList(),//
                    new SelectColumnsTransformSpec(selection.columns().getSelected()));
            final RagNode selectCols = graph.addNode(selectColsTransform);
            relinkPredecessorsToNewTarget(graph, root, selectCols, SPEC);
            graph.addEdge(selectCols, root, SPEC);
            SpecGraphBuilder.buildNumColumns(selectCols);
        }

        SpecGraphBuilder.buildNumColumns(root);
    }

    /**
     * For each predecessor (of the specified {@code edgeType}) of {@code
     * oldNode}: Replace the edge linking predecessor to {@code oldNode} with an
     * edge linking predecessor to {@code newNode}.
     */
    // TODO (TP): copied from RagBuilder (almost)
    //            refactor ...
    //            this should move to RagGraph probably ???
    private static void relinkPredecessorsToNewTarget(final RagGraph graph, final RagNode oldNode, final RagNode newNode, final RagEdgeType edgeType)
    {
        final List<RagEdge> edges = new ArrayList<>(oldNode.incomingEdges(edgeType));
        edges.forEach(edge -> graph.replaceEdgeTarget(edge, newNode));
    }

}
