package org.knime.core.table.virtual.aggregate;

import static java.util.UUID.randomUUID;
import static org.knime.core.table.schema.DataSpecs.DOUBLE;
import static org.knime.core.table.schema.DataSpecs.INT;
import static org.knime.core.table.schema.DataSpecs.STRING;
import static org.knime.core.table.virtual.graph.exec.CapExecutor.createRowAccessible;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.knime.core.table.RowAccessiblesTestUtils;
import org.knime.core.table.access.BufferedAccesses;
import org.knime.core.table.access.DoubleAccess.DoubleReadAccess;
import org.knime.core.table.access.DoubleAccess.DoubleWriteAccess;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.cursor.Cursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.row.Selection;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.VirtualTable;
import org.knime.core.table.virtual.exec.LazyVirtualTableExecutor;
import org.knime.core.table.virtual.graph.VirtualTableExamples;
import org.knime.core.table.virtual.graph.cap.CapBuilder;
import org.knime.core.table.virtual.graph.cap.CursorAssemblyPlan;
import org.knime.core.table.virtual.graph.rag.RagBuilder;
import org.knime.core.table.virtual.graph.rag.RagNode;
import org.knime.core.table.virtual.graph.util.ReadAccessUtils;
import org.knime.core.table.virtual.spec.AggregateTransformSpec;
import org.knime.core.table.virtual.spec.SourceTableProperties;

public class AggregatePlayground {

    public static VirtualTable vtAggregateMax(final UUID[] sourceIdentifiers, final RowAccessible[] sources) {
        final VirtualTable table = new VirtualTable(sourceIdentifiers[0], new SourceTableProperties(sources[0]));
        return table.aggregate(new int[]{2}, new MaxDouble());
    }

    public static VirtualTable vtAggregateMax() {
        return vtAggregateMax(new UUID[]{randomUUID(), randomUUID()}, dataAggregateMax());
    }

    public static RowAccessible[] dataAggregateMax() {
        final ColumnarSchema schema1 = ColumnarSchema.of(INT, DOUBLE, DOUBLE, STRING);
        final Object[][] values1 = new Object[][]{ //
                new Object[]{1, 0.5, 1.0, "First"}, //
                new Object[]{2, 1.2, 4.0, "Second"}, //
                new Object[]{3, 0.7, 3.0, "Third"}, //
                new Object[]{4, 4.9, 0.2, "Fourth"}, //
                new Object[]{5, 4.7, 3.0, "Fifth"}, //
                new Object[]{6, 1.0, 3.8, "Sixth"}, //
                new Object[]{7, 3.3, 3.2, "Seventh"}, //
        };
        return new RowAccessible[]{
                RowAccessiblesTestUtils.createRowAccessibleFromRowWiseValues(schema1, values1),
        };
    }


    public static class MaxDouble implements AggregateTransformSpec.AggregatorFactory<MaxDouble.Agg>
    {
        @Override
        public Supplier<Agg> supplier() {
            return Agg::new;
        }

        @Override
        public Consumer<Agg> accumulator(ReadAccess[] inputs) {
            final int expectedNumInputs = 1;
            if (inputs == null) {
                throw new NullPointerException();
            } else if (inputs.length != expectedNumInputs) {
                throw new IllegalArgumentException(
                        "expected " + expectedNumInputs + " inputs (instead of " + inputs.length + ")");
            } else if (!(inputs[0] instanceof DoubleReadAccess input)) {
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
            double max = Double.NEGATIVE_INFINITY;

            void aggregate(final double value) {
                max = Math.max(max, value);
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

                public AggCursor(final Selection selection)
                {
                    canForward = selection.rows().allSelected(0, 1) && selection.columns().isSelected(0);
                }

                @Override
                public boolean forward() {
                    if (!canForward)
                        return false;

                    ((DoubleWriteAccess)m_access.getBufferedAccess(0)).setDoubleValue(agg.max);
                    canForward = false;
                    return true;
                }

                @Override
                public void close() {
                }
            }
        }
    }


    public static void main(String[] args) {
        final UUID[] sourceIdentifiers = createSourceIds(1);
        final RowAccessible[] accessibles = dataAggregateMax();
        final VirtualTable table = vtAggregateMax(sourceIdentifiers, accessibles);

        // ----------------------------------------------
        // create accessible

        final Map<UUID, RowAccessible> uuidRowAccessibleMap = new HashMap<>();
        for (int i = 0; i < sourceIdentifiers.length; ++i) {
            uuidRowAccessibleMap.put(sourceIdentifiers[i], accessibles[i]);
        }

        final RowAccessible rows = new LazyVirtualTableExecutor(table.getProducingTransform()).execute(uuidRowAccessibleMap).get(0);

//        final List<RagNode> orderedRag = RagBuilder.createOrderedRag(table);
//        final ColumnarSchema schema = RagBuilder.createSchema(orderedRag);
//        final CursorAssemblyPlan cursorAssemblyPlan = CapBuilder.createCursorAssemblyPlan(orderedRag);
//        final RowAccessible rows = createRowAccessible(schema, cursorAssemblyPlan, uuidRowAccessibleMap);

        try (final Cursor<ReadAccessRow> cursor = rows.createCursor()) {
            while (cursor.forward()) {
                System.out.print("a = ");
                for (int i = 0; i < cursor.access().size(); i++)
                    System.out.print(ReadAccessUtils.toString(cursor.access().getAccess(i)) + ", ");
                System.out.println();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static UUID[] createSourceIds(final int n) {
        final UUID[] ids = new UUID[n];
        Arrays.setAll(ids, i -> UUID.randomUUID());
        return ids;
    }
}
