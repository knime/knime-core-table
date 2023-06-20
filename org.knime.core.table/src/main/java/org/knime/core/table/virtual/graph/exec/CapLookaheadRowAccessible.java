package org.knime.core.table.virtual.graph.exec;

import java.util.List;

import org.knime.core.table.cursor.LookaheadCursor;
import org.knime.core.table.row.LookaheadRowAccessible;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.row.Selection;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.graph.cap.CursorAssemblyPlan;

class CapLookaheadRowAccessible extends CapRowAccessible implements LookaheadRowAccessible {

    CapLookaheadRowAccessible(ColumnarSchema schema, CursorAssemblyPlan cap, List<RowAccessible> sources) {
        super(schema, cap, sources);
    }

    @Override
    public LookaheadCursor<ReadAccessRow> createCursor() {
        return new CapLookaheadCursor(assembleConsumer());
    }

    @Override
    public LookaheadCursor<ReadAccessRow> createCursor(final Selection selection) {
        // TODO: What should happen here? The CAP already has column/row selection baked
        //   in. We would have to create a new comp graph. This should probably not be done
        //   here, though...
        if (selection.allSelected()) {
            return createCursor();
        }
        throw new UnsupportedOperationException();
    }
}
