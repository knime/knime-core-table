package org.knime.core.table.virtual.exec;

import static org.knime.core.table.virtual.graph.exec.CapExecutor.createRowAccessible;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.TableTransform;
import org.knime.core.table.virtual.graph.cap.CapBuilder;
import org.knime.core.table.virtual.graph.cap.CursorAssemblyPlan;
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
        final RowAccessible rows = createRowAccessible(schema, cursorAssemblyPlan, inputs);
        return List.of(rows);
    }
}
