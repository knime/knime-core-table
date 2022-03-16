package org.knime.core.table.virtual.graph.exec;

import static org.knime.core.table.virtual.graph.cap.CapNodeType.SOURCE;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.graph.cap.CapNode;
import org.knime.core.table.virtual.graph.cap.CapNodeSource;
import org.knime.core.table.virtual.graph.cap.CursorAssemblyPlan;

public class CapExecutor {

    public static RowAccessible createRowAccessible(
            final ColumnarSchema schema,
            final CursorAssemblyPlan cap,
            final Map<UUID, RowAccessible> uuidRowAccessibleMap ) {

        final List<RowAccessible> sources = new ArrayList<>();
        for (CapNode node : cap.nodes()) {
            if (node.type() == SOURCE) {
                final UUID uuid = ((CapNodeSource)node).uuid();
                final RowAccessible a = uuidRowAccessibleMap.get(uuid);
                if (a == null)
                    throw new IllegalArgumentException("No RowAccessible found for UUID " + uuid );
                sources.add(a);
            }
        }
        return cap.supportsLookahead() //
                ? new CapLookaheadRowAccessible(schema, cap, sources) //
                : new CapRowAccessible(schema, cap, sources);
    }
}
