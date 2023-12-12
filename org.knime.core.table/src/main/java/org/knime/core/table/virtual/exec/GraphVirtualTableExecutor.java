package org.knime.core.table.virtual.exec;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;

import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.row.RowWriteAccessible;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.TableTransform;
import org.knime.core.table.virtual.graph.exec.CapExecutor;
import org.knime.core.table.virtual.graph.rag.RagBuilder;
import org.knime.core.table.virtual.graph.rag.RagGraph;
import org.knime.core.table.virtual.graph.rag.RagGraphProperties;
import org.knime.core.table.virtual.graph.rag.RagNode;
import org.knime.core.table.virtual.graph.rag.SpecGraphBuilder;
import org.knime.core.table.virtual.spec.SourceTableProperties.CursorType;

public class GraphVirtualTableExecutor implements VirtualTableExecutor {

    private final RagGraph specGraph;
    private final ColumnarSchema schema;
    private final CursorType supportedCursorType;

    public GraphVirtualTableExecutor(final TableTransform leafTransform)
    {
        specGraph = SpecGraphBuilder.buildSpecGraph(leafTransform);
        final List<RagNode> orderedRag = RagBuilder.createOrderedRag(specGraph);
        schema = RagBuilder.createSchema(orderedRag);
        supportedCursorType = RagGraphProperties.supportedCursorType(orderedRag);
    }

    @Override
    public List<RowAccessible> execute(Map<UUID, RowAccessible> inputs) {
        final RowAccessible rows = CapExecutor.createRowAccessible(specGraph, schema, supportedCursorType, inputs, true);
        return List.of(rows);
    }

    /**
     * Run the comp graph, reading from the provided {@code inputs} and writing
     * to the provided {@code outputs}.
     *
     * @param inputs
     * @param outputs
     * @throws CancellationException if the computation was cancelled
     * @throws CompletionException if the computation threw an exception
     */
    public void execute(//
            Map<UUID, RowAccessible> inputs,//
            Map<UUID, RowWriteAccessible> outputs//
    ) throws CancellationException, CompletionException {
        CapExecutor.execute(specGraph, inputs, outputs);
    }
}
