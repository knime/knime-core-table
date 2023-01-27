package org.knime.core.table.virtual.aggregate;

import static java.util.UUID.randomUUID;
import static org.knime.core.table.schema.DataSpecs.DOUBLE;
import static org.knime.core.table.schema.DataSpecs.INT;
import static org.knime.core.table.schema.DataSpecs.STRING;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.knime.core.table.RowAccessiblesTestUtils;
import org.knime.core.table.cursor.Cursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.VirtualTable;
import org.knime.core.table.virtual.exec.LazyVirtualTableExecutor;
import org.knime.core.table.virtual.graph.util.ReadAccessUtils;
import org.knime.core.table.virtual.spec.SourceTableProperties;

public class AggregatePlayground {

    public static VirtualTable vtAggregateCount(final UUID[] sourceIdentifiers, final RowAccessible[] sources) {
        final VirtualTable table = new VirtualTable(sourceIdentifiers[0], new SourceTableProperties(sources[0]));
        return table.aggregate(new int[]{2}, AggregateDoubleToInt.count());
    }

    public static VirtualTable vtAggregateSum(final UUID[] sourceIdentifiers, final RowAccessible[] sources) {
        final VirtualTable table = new VirtualTable(sourceIdentifiers[0], new SourceTableProperties(sources[0]));
        return table.aggregate(new int[]{2}, AggregateDouble.sum());
    }

    public static VirtualTable vtAggregateAvg(final UUID[] sourceIdentifiers, final RowAccessible[] sources) {
        final VirtualTable table = new VirtualTable(sourceIdentifiers[0], new SourceTableProperties(sources[0]));
        return table.aggregate(new int[]{2}, AggregateDouble.average());
    }

    public static VirtualTable vtAggregateMin(final UUID[] sourceIdentifiers, final RowAccessible[] sources) {
        final VirtualTable table = new VirtualTable(sourceIdentifiers[0], new SourceTableProperties(sources[0]));
        return table.aggregate(new int[]{2}, AggregateDouble.min());
    }

    public static VirtualTable vtAggregateMax(final UUID[] sourceIdentifiers, final RowAccessible[] sources) {
        final VirtualTable table = new VirtualTable(sourceIdentifiers[0], new SourceTableProperties(sources[0]));
        return table.aggregate(new int[]{2}, AggregateDouble.max());
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



    public static void main(String[] args) {
        final UUID[] sourceIdentifiers = createSourceIds(1);
        final RowAccessible[] accessibles = dataAggregateMax();

        // ----------------------------------------------
        // create accessible

        final Map<UUID, RowAccessible> uuidRowAccessibleMap = new HashMap<>();
        for (int i = 0; i < sourceIdentifiers.length; ++i) {
            uuidRowAccessibleMap.put(sourceIdentifiers[i], accessibles[i]);
        }

        System.out.println("COUNT:");
        printTable(uuidRowAccessibleMap, vtAggregateCount(sourceIdentifiers, accessibles));
        System.out.println("SUM:");
        printTable(uuidRowAccessibleMap, vtAggregateSum(sourceIdentifiers, accessibles));
        System.out.println("AVG:");
        printTable(uuidRowAccessibleMap, vtAggregateAvg(sourceIdentifiers, accessibles));
        System.out.println("MIN:");
        printTable(uuidRowAccessibleMap, vtAggregateMin(sourceIdentifiers, accessibles));
        System.out.println("MAX:");
        printTable(uuidRowAccessibleMap, vtAggregateMax(sourceIdentifiers, accessibles));
    }

    private static void printTable(Map<UUID, RowAccessible> uuidRowAccessibleMap, VirtualTable table) {
        final RowAccessible rows = new LazyVirtualTableExecutor(table.getProducingTransform()).execute(uuidRowAccessibleMap).get(0);
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



    public static void main2(String[] args) {
        final UUID[] sourceIdentifiers = createSourceIds(1);
        final RowAccessible[] accessibles = dataAggregateMax();
        final VirtualTable table = vtAggregateMax(sourceIdentifiers, accessibles);

        // ----------------------------------------------
        // create accessible

        final Map<UUID, RowAccessible> uuidRowAccessibleMap = new HashMap<>();
        for (int i = 0; i < sourceIdentifiers.length; ++i) {
            uuidRowAccessibleMap.put(sourceIdentifiers[i], accessibles[i]);
        }

//        final List<RagNode> orderedRag = RagBuilder.createOrderedRag(table);
//        final ColumnarSchema schema = RagBuilder.createSchema(orderedRag);
//        final CursorAssemblyPlan cursorAssemblyPlan = CapBuilder.createCursorAssemblyPlan(orderedRag);
//        final RowAccessible rows = createRowAccessible(schema, cursorAssemblyPlan, uuidRowAccessibleMap);

        printTable(uuidRowAccessibleMap, table);
    }

    private static UUID[] createSourceIds(final int n) {
        final UUID[] ids = new UUID[n];
        Arrays.setAll(ids, i -> UUID.randomUUID());
        return ids;
    }
}
