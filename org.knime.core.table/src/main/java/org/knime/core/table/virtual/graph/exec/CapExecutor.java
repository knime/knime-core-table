package org.knime.core.table.virtual.graph.exec;

import static org.knime.core.table.virtual.graph.cap.CapNodeType.MATERIALIZE;
import static org.knime.core.table.virtual.graph.cap.CapNodeType.SOURCE;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;

import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.row.RowWriteAccessible;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.graph.cap.CapNode;
import org.knime.core.table.virtual.graph.cap.CapNodeMaterialize;
import org.knime.core.table.virtual.graph.cap.CapNodeSource;
import org.knime.core.table.virtual.graph.cap.CursorAssemblyPlan;
import org.knime.core.table.virtual.graph.rag.RagGraph;

public class CapExecutor {

    /**
     *
     * @param specGraph
     * @param schema
     * @param cap
     * @param uuidRowAccessibleMap
     * @param useRandomAccess if {@code true}, then a {@code RandomRowAccessible} will be created if the {@code cap} supports it.
     * @return
     */
    public static RowAccessible createRowAccessible(
            final RagGraph specGraph,
            final ColumnarSchema schema,
            final CursorAssemblyPlan cap,
            final Map<UUID, RowAccessible> uuidRowAccessibleMap,
            final boolean useRandomAccess) {

        if (useRandomAccess && cap.supportsRandomAccess()) {
            return new CapRandomRowAccessible(specGraph, schema, cap, uuidRowAccessibleMap);
        } else if (cap.supportsLookahead()) {
            return new CapLookaheadRowAccessible(specGraph, schema, cap, uuidRowAccessibleMap);
        } else {
            return new CapRowAccessible(specGraph, schema, cap, uuidRowAccessibleMap);
        }
    }

    public static void execute(final CursorAssemblyPlan cap, //
            final Map<UUID, RowAccessible> uuidRowAccessibleMap, //
            final Map<UUID, RowWriteAccessible> uuidRowWriteAccessibleMap //
    ) throws CompletionException, CancellationException {

        try {
            final List<RowAccessible> sources = CapExecutorUtils.getSources(cap, uuidRowAccessibleMap);

            // TODO (TP) extract as method getSinks() in CapExecutorUtils
            final List<RowWriteAccessible> sinks = new ArrayList<>();
            for (CapNode node : cap.nodes()) {
                if (node.type() == MATERIALIZE) {
                    final UUID uuid = ((CapNodeMaterialize)node).uuid();
                    final RowWriteAccessible a = uuidRowWriteAccessibleMap.get(uuid);
                    if (a == null) {
                        throw new IllegalArgumentException("No RowWriteAccessible found for UUID " + uuid);
                    }
                    // TODO AP-20400: check for compatibility (currently disabled because of the void RowID column
                    // in the ColumnarRearranger
//                    if (!Objects.equals(a.getSchema(), schemas.get(uuid))) {
//                        throw new IllegalArgumentException(
//                            "RowWriteAccessible for UUID " + uuid + " does not match expected ColumnarSchema");
//                    }
                    sinks.add(a);
                }
            }
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
