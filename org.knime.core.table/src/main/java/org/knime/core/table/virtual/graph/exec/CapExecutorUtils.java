package org.knime.core.table.virtual.graph.exec;

import static org.knime.core.table.virtual.graph.cap.CapNodeType.MATERIALIZE;
import static org.knime.core.table.virtual.graph.cap.CapNodeType.SOURCE;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.row.RowWriteAccessible;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.graph.cap.CapNode;
import org.knime.core.table.virtual.graph.cap.CapNodeMaterialize;
import org.knime.core.table.virtual.graph.cap.CapNodeSource;
import org.knime.core.table.virtual.graph.cap.CursorAssemblyPlan;

class CapExecutorUtils {

    /**
     * Get list of sources occurring in {@code CursorAssemblyPlan}. The list
     * contains one source for each {@code CapNodeSource} in the order in which
     * they occur in the CAP.
     */
    static List<RowAccessible> getSources(final CursorAssemblyPlan cap, final Map<UUID, RowAccessible> uuidRowAccessibleMap) {
        final List<RowAccessible> sources = new ArrayList<>();
        final Map<UUID, ColumnarSchema> schemas = cap.schemas();
        for (CapNode node : cap.nodes()) {
            if (node.type() == SOURCE) {
                final UUID uuid = ((CapNodeSource)node).uuid();
                final RowAccessible a = uuidRowAccessibleMap.get(uuid);
                if (a == null) {
                    throw new IllegalArgumentException("No RowAccessible found for UUID " + uuid);
                }
                if (!Objects.equals(a.getSchema(), schemas.get(uuid))) {
                    throw new IllegalArgumentException(
                            "RowAccessible for UUID " + uuid + " does not match expected ColumnarSchema");
                }
                sources.add(a);
            }
        }
        return sources;
    }


    /**
     * Get list of sinks occurring in {@code CursorAssemblyPlan}. The list
     * contains one sink for each {@code CapNodeMaterialize} in the order in
     * which they occur in the CAP.
     */
    static List<RowWriteAccessible> getSinks(final CursorAssemblyPlan cap, final Map<UUID, RowWriteAccessible> uuidRowWriteAccessibleMap) {
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
        return sinks;
    }
}
