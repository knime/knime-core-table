package org.knime.core.table.virtual.aggregate;

import static org.knime.core.table.schema.DataSpecs.INT;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.ObjDoubleConsumer;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;

import org.knime.core.table.access.BufferedAccesses;
import org.knime.core.table.access.DoubleAccess;
import org.knime.core.table.access.IntAccess.IntWriteAccess;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.cursor.Cursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.row.Selection;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.spec.AggregateTransformSpec.AggregatorFactory;

public class AggregateDoubleToInt<R> implements AggregatorFactory<R> {

    public static AggregateDoubleToInt<Value> count() {
        return reduce(0, (i, d) -> i + 1);
    }

    private static final int expectedNumInputs = 1;

    private final Supplier<R> supplier;
    private final ObjDoubleConsumer<R> accumulator;
    private final BiConsumer<R, R> combiner;
    private final ToIntFunction<R> finisher;

    public AggregateDoubleToInt(
            Supplier<R> supplier,
            ObjDoubleConsumer<R> accumulator,
            BiConsumer<R,R> combiner,
            ToIntFunction<R> finisher) {
        this.supplier = supplier;
        this.accumulator = accumulator;
        this.combiner = combiner;
        this.finisher = finisher;
    }

    public static class Value {
        private int value;

        public Value(int value) {
            this.value = value;
        }

        public int get() {
            return value;
        }

        public void set(int value) {
            this.value = value;
        }
    }

    public interface IntAndDoubleToIntBiFunction {
        int applyAsInt(int i, double d);
    }

    public static AggregateDoubleToInt<Value> reduce(int identity, IntAndDoubleToIntBiFunction op) {
        return new AggregateDoubleToInt<>( //
                () -> new Value(identity), //
                (r, d) -> r.set(op.applyAsInt(r.get(), d)), //
                (r, r2) -> r.set(op.applyAsInt(r.get(), r2.get())), //
                Value::get);
    }

    @Override
    public Supplier<R> supplier() {
        return supplier;
    }

    @Override
    public Consumer<R> accumulator(final ReadAccess[] inputs) {
        if (inputs == null) {
            throw new NullPointerException();
        } else if (inputs.length != expectedNumInputs) {
            throw new IllegalArgumentException(
                    "expected " + expectedNumInputs + " inputs (instead of " + inputs.length + ")");
        } else if (!(inputs[0] instanceof DoubleAccess.DoubleReadAccess input)) {
            throw new IllegalArgumentException("expected DOUBLE input");
        } else {
            return r -> accumulator.accept(r, input.getDoubleValue());
        }
    }

    @Override
    public AggRowAccessible finisher() {
        return new AggRowAccessible();
    }

    @Override
    public ColumnarSchema getOutputSchema() {
        return ColumnarSchema.of(INT);
    }

    class AggRowAccessible implements RowAccessible, Consumer<R> {
        private R r;

        @Override
        public void accept(final R r) {
            this.r = r;
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

                final IntWriteAccess a = m_access.getBufferedAccess(0);
                a.setIntValue(finisher.applyAsInt(r));
                canForward = false;
                return true;
            }

            @Override
            public void close() {
            }
        }
    }
}
