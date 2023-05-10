package org.knime.core.table.virtual.graph.exec;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.knime.core.table.cursor.RandomAccessCursor;
import org.knime.core.table.row.RandomRowAccessible;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.row.Selection;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.graph.cap.CursorAssemblyPlan;

class CapRandomRowAccessible implements RandomRowAccessible {

    private final ColumnarSchema schema;

    private final CursorAssemblyPlan cap;

    private final List<RowAccessible> sources;

    CapRandomRowAccessible(final ColumnarSchema schema, final CursorAssemblyPlan cap, final List<RowAccessible> sources) {
        this.schema = schema;
        this.cap = cap;
        this.sources = sources;
    }

    @Override
    public ColumnarSchema getSchema() {
        return schema;
    }

    @Override
    public RandomAccessCursor<ReadAccessRow> createCursor() {
        return new CapRandomAccessCursor(assembleConsumer(), size());
    }

    @Override
    public RandomAccessCursor<ReadAccessRow> createCursor(Selection selection) {
        // TODO: What should happen here? The CAP already has column/row selection baked
        //   in. We would have to create a new comp graph. This should probably not be done
        //   here, though...
        if (selection.allSelected()) {
            return createCursor();
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public long size() {
        return cap.numRows();
    }

    @Override
    public void close() throws IOException {
        // TODO ?
    }

    RandomAccessNodeImpConsumer assembleConsumer() {
        return new AssembleRandomAccessibleNodeImps(cap.nodes(), sources).getConsumer();
    }
}
