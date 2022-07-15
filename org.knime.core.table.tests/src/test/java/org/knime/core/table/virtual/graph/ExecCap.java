package org.knime.core.table.virtual.graph;

import static org.knime.core.table.RowAccessiblesTestUtils.toLookahead;
import static org.knime.core.table.virtual.graph.exec.CapExecutor.createRowAccessible;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.knime.core.table.cursor.Cursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.VirtualTable;
import org.knime.core.table.virtual.graph.cap.CapBuilder;
import org.knime.core.table.virtual.graph.cap.CursorAssemblyPlan;
import org.knime.core.table.virtual.graph.rag.RagBuilder;
import org.knime.core.table.virtual.graph.rag.RagNode;
import org.knime.core.table.virtual.graph.util.ReadAccessUtils;

public class ExecCap {

    public static void main(final String[] args) {
//        final UUID[] sourceIdentifiers = createSourceIds(1);
//        final RowAccessible[] accessibles = VirtualTableExamples.dataMinimal();
//        final VirtualTable table = VirtualTableExamples.vtMinimal(sourceIdentifiers, accessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(2);
//        final RowAccessible[] accessibles = VirtualTableExamples.dataAppend();
//        final VirtualTable table = VirtualTableExamples.vtAppend(sourceIdentifiers, accessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(2);
//        final RowAccessible[] accessibles = toLookahead(VirtualTableExamples.dataAppendAndSlice());
//        final VirtualTable table = VirtualTableExamples.vtAppendAndSlice(sourceIdentifiers, accessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(1);
//        final RowAccessible[] accessibles = VirtualTableExamples.dataForkJoin();
//        final VirtualTable table = VirtualTableExamples.vtForkJoin(sourceIdentifiers, accessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(1);
//        final RowAccessible[] accessibles = toLookahead(VirtualTableExamples.dataForkJoinLookALike());
//        final VirtualTable table = VirtualTableExamples.vtForkJoinLookALike(sourceIdentifiers, accessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(2);
//        final RowAccessible[] accessibles = VirtualTableExamples.dataConcatenate();
//        final VirtualTable table = VirtualTableExamples.vtConcatenate(sourceIdentifiers, accessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(3);
//        final RowAccessible[] accessibles = VirtualTableExamples.dataConcatenateAndSlice();
//        final VirtualTable table = VirtualTableExamples.vtConcatenateAndSlice(sourceIdentifiers, accessibles);
//        final VirtualTable table = VirtualTableExamples.vtConcatenateAndSliceSingleTable(sourceIdentifiers, accessibles);
//        final VirtualTable table = VirtualTableExamples.vtConcatenateAndSliceFullSingleTable(sourceIdentifiers, accessibles);
//        final VirtualTable table = VirtualTableExamples.vtConcatenateAndSliceFullTable(sourceIdentifiers, accessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(2);
//        final RowAccessible[] accessibles = VirtualTableExamples.dataAppendMissing();
//        final VirtualTable table = VirtualTableExamples.vtAppendMissing(sourceIdentifiers, accessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(1);
//        final RowAccessible[] accessibles = VirtualTableExamples.dataSimpleMap();
//        final VirtualTable table = VirtualTableExamples.vtSimpleMap(sourceIdentifiers, accessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(1);
//        final RowAccessible[] accessibles = VirtualTableExamples.dataSimpleRowFilter();
//        final VirtualTable table = VirtualTableExamples.vtSimpleRowFilter(sourceIdentifiers, accessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(1);
//        final RowAccessible[] accessibles = VirtualTableExamples.dataConsecutiveRowFilters();
//        final VirtualTable table = VirtualTableExamples.vtConsecutiveRowFilters(sourceIdentifiers, accessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(1);
//        final RowAccessible[] accessibles = VirtualTableExamples.dataMapsAndFilters();
//        final VirtualTable table = VirtualTableExamples.vtMapsAndFilters(sourceIdentifiers, accessibles);

        final UUID[] sourceIdentifiers = createSourceIds(2);
        final RowAccessible[] accessibles = VirtualTableExamples.dataFiltersMapAndConcatenate();
        final VirtualTable table = VirtualTableExamples.vtFiltersMapAndConcatenate(sourceIdentifiers, accessibles);

        // ----------------------------------------------
        // create accessible

        final Map<UUID, RowAccessible> uuidRowAccessibleMap = new HashMap<>();
        for (int i = 0; i < sourceIdentifiers.length; ++i) {
            uuidRowAccessibleMap.put(sourceIdentifiers[i], accessibles[i]);
        }

        final List<RagNode> orderedRag = RagBuilder.createOrderedRag(table);
        final ColumnarSchema schema = RagBuilder.createSchema(orderedRag);
        final CursorAssemblyPlan cursorAssemblyPlan = CapBuilder.createCursorAssemblyPlan(orderedRag);
        final RowAccessible rows = createRowAccessible(schema, cursorAssemblyPlan, uuidRowAccessibleMap);

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
