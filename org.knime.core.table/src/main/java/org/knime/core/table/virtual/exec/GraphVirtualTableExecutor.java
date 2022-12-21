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
import org.knime.core.table.virtual.graph.cap.CapBuilder;
import org.knime.core.table.virtual.graph.cap.CursorAssemblyPlan;
import org.knime.core.table.virtual.graph.exec.CapExecutor;
import org.knime.core.table.virtual.graph.rag.RagBuilder;
import org.knime.core.table.virtual.graph.rag.RagNode;

public class GraphVirtualTableExecutor implements VirtualTableExecutor {

    private final ColumnarSchema schema;
    private final CursorAssemblyPlan cursorAssemblyPlan;

    public GraphVirtualTableExecutor(final TableTransform leafTransform)
    {
        final List<RagNode> orderedRag = RagBuilder.createOrderedRag(leafTransform);
        schema = RagBuilder.createSchema(orderedRag);
        cursorAssemblyPlan = CapBuilder.createCursorAssemblyPlan(orderedRag);
    }

    @Override
    public List<RowAccessible> execute(Map<UUID, RowAccessible> inputs) {
        final RowAccessible rows = CapExecutor.createRowAccessible(schema, cursorAssemblyPlan, inputs);
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
        CapExecutor.execute(cursorAssemblyPlan, inputs, outputs);
    }
}
