package org.knime.core.table.virtual.graph.exec;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;

import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.row.RowWriteAccessible;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.graph.cap.CapBuilder;
import org.knime.core.table.virtual.graph.cap.CursorAssemblyPlan;
import org.knime.core.table.virtual.graph.rag.RagBuilder;
import org.knime.core.table.virtual.graph.rag.RagGraph;
import org.knime.core.table.virtual.graph.rag.RagNode;
import org.knime.core.table.virtual.spec.SourceTableProperties.CursorType;

public class CapExecutor {

    /**
     * @param specGraph
     * @param schema
     * @param cursorType
     * @param uuidRowAccessibleMap
     * @return
     */
    public static RowAccessible createRowAccessible(
            final RagGraph specGraph,
            final ColumnarSchema schema,
            final CursorType cursorType,
            final Map<UUID, RowAccessible> uuidRowAccessibleMap) {
        return switch (cursorType) {
            case BASIC -> new CapRowAccessible(specGraph, schema, uuidRowAccessibleMap);
            case LOOKAHEAD -> new CapLookaheadRowAccessible(specGraph, schema, uuidRowAccessibleMap);
            case RANDOMACCESS -> new CapRandomRowAccessible(specGraph, schema, uuidRowAccessibleMap);
        };
    }

    public static void execute(
            final RagGraph specGraph,
            final Map<UUID, RowAccessible> uuidRowAccessibleMap, //
            final Map<UUID, RowWriteAccessible> uuidRowWriteAccessibleMap //
    ) throws CompletionException, CancellationException {

        try {
            final List<RagNode> orderedRag = RagBuilder.createOrderedRag(specGraph);
            final CursorAssemblyPlan cap = CapBuilder.createCursorAssemblyPlan(orderedRag);
            final List<RowAccessible> sources = CapExecutorUtils.getSources(cap, uuidRowAccessibleMap);
            final List<RowWriteAccessible> sinks = CapExecutorUtils.getSinks(cap, uuidRowWriteAccessibleMap);

            final NodeImp terminator = new AssembleNodeImps(cap.nodes(), sources, sinks).getTerminator();
            terminator.create();
            terminator.forward();
            terminator.close();
        } catch (CancellationException | CompletionException ex) {
            throw ex;
        } catch (Throwable ex) {
            throw new CompletionException(ex);
        }
    }
}
