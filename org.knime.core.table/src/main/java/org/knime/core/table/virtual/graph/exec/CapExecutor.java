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
import org.knime.core.table.virtual.spec.SourceTableProperties;

public class CapExecutor {

    /**
     * @param specGraph
     * @param schema
     * @param supportedCursorType
     * @param uuidRowAccessibleMap
     * @param useRandomAccess      if {@code true}, then a {@code RandomRowAccessible} will be created if the {@code cap} supports it.
     * @return
     */
    public static RowAccessible createRowAccessible(
            final RagGraph specGraph,
            final ColumnarSchema schema,
            final SourceTableProperties.CursorType supportedCursorType,
            final Map<UUID, RowAccessible> uuidRowAccessibleMap,
            final boolean useRandomAccess) {

        if (useRandomAccess && supportedCursorType.supportsRandomAccess()) {
            return new CapRandomRowAccessible(specGraph, schema, uuidRowAccessibleMap);
        } else if (supportedCursorType.supportsLookahead()) {
            return new CapLookaheadRowAccessible(specGraph, schema, uuidRowAccessibleMap);
        } else {
            return new CapRowAccessible(specGraph, schema, uuidRowAccessibleMap);
        }
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
