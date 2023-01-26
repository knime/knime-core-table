package org.knime.core.table.virtual.aggregate;

import static org.knime.core.table.schema.DataSpecs.DOUBLE;

import java.util.function.Consumer;
import java.util.function.Supplier;

import org.knime.core.table.access.BufferedAccesses;
import org.knime.core.table.access.DoubleAccess;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.cursor.Cursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.row.Selection;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.spec.AggregateTransformSpec.AggregatorFactory;

public class AvgDouble implements AggregatorFactory<AvgDouble.Agg> {

    private static final int expectedNumInputs = 1;

    @Override
    public Supplier<Agg> supplier() {
        return Agg::new;
    }

    @Override
    public Consumer<Agg> accumulator(final ReadAccess[] inputs) {
        if (inputs == null) {
            throw new NullPointerException();
        } else if (inputs.length != expectedNumInputs) {
            throw new IllegalArgumentException(
                    "expected " + expectedNumInputs + " inputs (instead of " + inputs.length + ")");
        } else if (!(inputs[0] instanceof DoubleAccess.DoubleReadAccess input)) {
            throw new IllegalArgumentException("expected DOUBLE input");
        } else {
            return a -> a.aggregate(input.getDoubleValue());
        }
    }

    @Override
    public AggRowAccessible finisher() {
        return new AggRowAccessible();
    }

    @Override
    public ColumnarSchema getOutputSchema() {
        return ColumnarSchema.of(DOUBLE);
    }

    static class Agg {
        double sum = 0.0;
        int n = 0;

        void aggregate(final double value) {
            sum += value;
            ++n;
        }
    }

    class AggRowAccessible implements RowAccessible, Consumer<Agg> {
        private Agg agg;

        @Override
        public void accept(final Agg agg) {
            this.agg = agg;
        }

        @Override
        public ColumnarSchema getSchema() {
            return getOutputSchema();
        }

        @Override
        public Cursor<ReadAccessRow> createCursor() {
            return createCursor(Selection.all());
        }

        @Override
        public Cursor<ReadAccessRow> createCursor(Selection selection) {
            return new AggCursor(selection);
        }

        @Override
        public long size() {
            return 1;
        }

        @Override
        public void close() {
        }

        class AggCursor implements Cursor<ReadAccessRow> {

            private BufferedAccesses.BufferedAccessRow m_access = BufferedAccesses.createBufferedAccessRow(getOutputSchema());

            private boolean canForward;

            @Override
            public ReadAccessRow access() {
                return m_access;
            }

            public AggCursor(final Selection selection) {
                canForward = selection.rows().allSelected(0, 1) && selection.columns().isSelected(0);
            }

            @Override
            public boolean forward() {
                if (!canForward)
                    return false;

                final DoubleAccess.DoubleWriteAccess a = m_access.getBufferedAccess(0);
                if (agg.n == 0)
                    a.setMissing();
                else
                    a.setDoubleValue(agg.sum / agg.n);

                canForward = false;
                return true;
            }

            @Override
            public void close() {
            }
        }
    }
}
