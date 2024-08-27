package org.knime.core.table.virtual.graph.rag3;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.knime.core.table.cursor.Cursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.row.RowWriteAccessible;
import org.knime.core.table.virtual.VirtualTable;
import org.knime.core.table.virtual.graph.VirtualTableTests;
import org.knime.core.table.virtual.graph.cap.CursorAssemblyPlan;
import org.knime.core.table.virtual.graph.util.ReadAccessUtils;

public class ExecCap3 {

    public static void main(String[] args) {
        UUID[] sinkIdentifiers = null;
        RowWriteAccessible[] sinkAccessibles = null;

//        final UUID[] sourceIdentifiers = createSourceIds(1);
//        final RowAccessible[] sourceAccessibles = VirtualTableTests.dataMinimal();
//        final VirtualTable table = VirtualTableTests.vtMinimal(sourceIdentifiers, sourceAccessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(2);
//        final RowAccessible[] sourceAccessibles = VirtualTableTests.dataAppend();
//        final VirtualTable table = VirtualTableTests.vtAppend(sourceIdentifiers, sourceAccessibles);

        final UUID[] sourceIdentifiers = createSourceIds(1);
        final RowAccessible[] sourceAccessibles = VirtualTableTests.dataMapsAndFilters();
        final VirtualTable table = VirtualTableTests.vtMapsAndFilters(sourceIdentifiers, sourceAccessibles);








        final Map<UUID, RowAccessible> uuidRowAccessibleMap = new HashMap<>();
        for (int i = 0; i < sourceIdentifiers.length; ++i) {
            uuidRowAccessibleMap.put(sourceIdentifiers[i], sourceAccessibles[i]);
        }

        // create CAP
        TableTransformGraph tableTransformGraph = TableTransformGraph.of(table.getProducingTransform());

        System.out.println(new DependencyGraph(tableTransformGraph));
        TableTransformGraph tableTransformGraph2 = tableTransformGraph.copy();
        System.out.println(new DependencyGraph(tableTransformGraph2));

        SpecGraph.BranchGraph depGraph = new SpecGraph.BranchGraph(tableTransformGraph2);
        CursorAssemblyPlan cap = SpecGraph.BuildCap.getCursorAssemblyPlan(depGraph);


        // create RowAccessible
        CapRowAccessible3 rows = new CapRowAccessible3(cap, table.getSchema(), uuidRowAccessibleMap);

        // print results:
        try (final Cursor<ReadAccessRow> cursor = rows.createCursor()) {
            while (cursor.forward()) {
                System.out.print("a = ");
                for (int i = 0; i < cursor.access().size(); i++) {
                    System.out.print(ReadAccessUtils.toString(cursor.access().getAccess(i)) + ", ");
                }
                System.out.println();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


//        -- > new CapRowAccessible( // final ColumnarSchema schema, //
//        final Map<UUID, RowAccessible> availableSources){

        }

        private static UUID[] createSourceIds ( final int n){
            final UUID[] ids = new UUID[n];
            Arrays.setAll(ids, i -> UUID.randomUUID());
            return ids;
        }
    }
