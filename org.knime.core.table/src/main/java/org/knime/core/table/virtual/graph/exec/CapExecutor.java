package org.knime.core.table.virtual.graph.exec;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;

import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.row.RowWriteAccessible;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.graph.cap.CursorAssemblyPlan;

public class CapExecutor {

    public static RowAccessible createRowAccessible(
            final ColumnarSchema schema,
            final CursorAssemblyPlan cap,
            final Map<UUID, RowAccessible> uuidRowAccessibleMap ) {

        final List<RowAccessible> sources = CapExecutorUtils.getSources(cap, uuidRowAccessibleMap);
        return cap.supportsLookahead() //
                ? new CapLookaheadRowAccessible(schema, cap, sources) //
                : new CapRowAccessible(schema, cap, sources);
    }

    public static void execute(final CursorAssemblyPlan cap, //
            final Map<UUID, RowAccessible> uuidRowAccessibleMap, //
            final Map<UUID, RowWriteAccessible> uuidRowWriteAccessibleMap //
    ) throws CompletionException, CancellationException {

        try {
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
