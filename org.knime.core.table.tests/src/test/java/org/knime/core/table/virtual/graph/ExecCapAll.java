package org.knime.core.table.virtual.graph;

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

public class ExecCapAll {

    public static void main(final String[] args) {
        {
            final UUID[] sourceIdentifiers = createSourceIds(1);
            final VirtualTable table = VirtualTableExamples.vtMinimal(sourceIdentifiers);
            final RowAccessible[] accessibles = VirtualTableExamples.dataMinimal();
            printResults("vtMinimal", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(1);
            final VirtualTable table = VirtualTableExamples.vtLinear(sourceIdentifiers);
            final RowAccessible[] accessibles = VirtualTableExamples.dataLinear();
            printResults("vtLinear", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(1);
            final VirtualTable table = VirtualTableExamples.vtForkJoin(sourceIdentifiers);
            final RowAccessible[] accessibles = VirtualTableExamples.dataForkJoin();
            printResults("vtForkJoin", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(1);
            final VirtualTable table = VirtualTableExamples.vtForkJoinLookALike(sourceIdentifiers);
            final RowAccessible[] accessibles = VirtualTableExamples.dataForkJoinLookALike();
            printResults("vtForkJoinLookALike", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(2);
            final VirtualTable table = VirtualTableExamples.vtAppend(sourceIdentifiers);
            final RowAccessible[] accessibles = VirtualTableExamples.dataAppend();
            printResults("vtAppend", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(2);
            final VirtualTable table = VirtualTableExamples.vtAppendAndAppendMissing(sourceIdentifiers);
            final RowAccessible[] accessibles = VirtualTableExamples.dataAppendAndAppendMissing();
            printResults("vtAppendAndAppendMissing", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(2);
            final VirtualTable table = VirtualTableExamples.vtConcatenate(sourceIdentifiers);
            final RowAccessible[] accessibles = VirtualTableExamples.dataConcatenate();
            printResults("vtConcatenate", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(2);
            final VirtualTable table = VirtualTableExamples.vtAppendMissing(sourceIdentifiers);
            final RowAccessible[] accessibles = VirtualTableExamples.dataAppendMissing();
            printResults("vtAppendMissing", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(1);
            final VirtualTable table = VirtualTableExamples.vtSimpleMap(sourceIdentifiers);
            final RowAccessible[] accessibles = VirtualTableExamples.dataSimpleMap();
            printResults("vtSimpleMap", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(1);
            final VirtualTable table = VirtualTableExamples.vtSimpleRowFilter(sourceIdentifiers);
            final RowAccessible[] accessibles = VirtualTableExamples.dataSimpleRowFilter();
            printResults("vtSimpleRowFilter", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(1);
            final VirtualTable table = VirtualTableExamples.vtConsecutiveRowFilters(sourceIdentifiers);
            final RowAccessible[] accessibles = VirtualTableExamples.dataConsecutiveRowFilters();
            printResults("vtConsecutiveRowFilters", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(1);
            final VirtualTable table = VirtualTableExamples.vtMapsAndFilters(sourceIdentifiers);
            final RowAccessible[] accessibles = VirtualTableExamples.dataMapsAndFilters();
            printResults("vtMapsAndFilters", sourceIdentifiers, table, accessibles);
        }
    }

    private static void printResults(
            final String exampleName,
            final UUID[] sourceIdentifiers,
            final VirtualTable table,
            final RowAccessible[] accessibles) {

        final Map<UUID, RowAccessible> uuidRowAccessibleMap = new HashMap<>();
        for (int i = 0; i < sourceIdentifiers.length; ++i) {
            uuidRowAccessibleMap.put(sourceIdentifiers[i], accessibles[i]);
        }

        final List<RagNode> orderedRag = RagBuilder.createOrderedRag(table);
        final CursorAssemblyPlan cursorAssemblyPlan = CapBuilder.createCursorAssemblyPlan(orderedRag);
        final ColumnarSchema schema = RagBuilder.createSchema(orderedRag);
        final RowAccessible rows = createRowAccessible(schema, cursorAssemblyPlan, uuidRowAccessibleMap);

        System.out.println(exampleName);
        System.out.println("------------------------");
        System.out.println(table.getSchema());
        System.out.println("------------------------");
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
        System.out.println();
        System.out.println();
    }

    private static UUID[] createSourceIds(final int n) {
        final UUID[] ids = new UUID[n];
        Arrays.setAll(ids, i -> UUID.randomUUID());
        return ids;
    }
}
