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
import org.knime.core.table.virtual.graph.rag.RagGraph;
import org.knime.core.table.virtual.graph.rag.RagNode;
import org.knime.core.table.virtual.graph.rag.SpecGraphBuilder;
import org.knime.core.table.virtual.graph.util.ReadAccessUtils;

public class ExecCapAll {

    public static void main(final String[] args) {
        {
            final UUID[] sourceIdentifiers = createSourceIds(1);
            final RowAccessible[] accessibles = VirtualTableExamples.dataMinimal();
            final VirtualTable table = VirtualTableExamples.vtMinimal(sourceIdentifiers, accessibles);
            printResults("vtMinimal", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(1);
            final RowAccessible[] accessibles = VirtualTableExamples.dataLinear();
            final VirtualTable table = VirtualTableExamples.vtLinear(sourceIdentifiers, accessibles);
            printResults("vtLinear", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(1);
            final RowAccessible[] accessibles = VirtualTableExamples.dataForkJoin();
            final VirtualTable table = VirtualTableExamples.vtForkJoin(sourceIdentifiers, accessibles);
            printResults("vtForkJoin", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(1);
            final RowAccessible[] accessibles = toLookahead(VirtualTableExamples.dataForkJoinLookALike());
            final VirtualTable table = VirtualTableExamples.vtForkJoinLookALike(sourceIdentifiers, accessibles);
            printResults("vtForkJoinLookALike", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(2);
            final RowAccessible[] accessibles = VirtualTableExamples.dataAppend();
            final VirtualTable table = VirtualTableExamples.vtAppend(sourceIdentifiers, accessibles);
            printResults("vtAppend", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(2);
            final RowAccessible[] accessibles = VirtualTableExamples.dataAppendAndFilterColumns();
            final VirtualTable table = VirtualTableExamples.vtAppendAndFilterColumns(sourceIdentifiers, accessibles);
            printResults("vtAppendAndFilterColumns", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(2);
            final RowAccessible[] accessibles = VirtualTableExamples.dataAppendAndSlice();
            final VirtualTable table = VirtualTableExamples.vtAppendAndSlice(sourceIdentifiers, accessibles);
            printResults("vtAppendAndSlice", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(2);
            final RowAccessible[] accessibles = VirtualTableExamples.dataAppendAndAppendMissing();
            final VirtualTable table = VirtualTableExamples.vtAppendAndAppendMissing(sourceIdentifiers, accessibles);
            printResults("vtAppendAndAppendMissing", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(2);
            final RowAccessible[] accessibles = VirtualTableExamples.dataConcatenate();
            final VirtualTable table = VirtualTableExamples.vtConcatenate(sourceIdentifiers, accessibles);
            printResults("vtConcatenate", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(3);
            final RowAccessible[] accessibles = VirtualTableExamples.dataConcatenateAndSlice();
            final VirtualTable table = VirtualTableExamples.vtConcatenateAndSlice(sourceIdentifiers, accessibles);
            printResults("vtConcatenateAndSlice", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(3);
            final RowAccessible[] accessibles = VirtualTableExamples.dataConcatenateAndSlice();
            final VirtualTable table = VirtualTableExamples.vtConcatenateAndSliceSingleTable(sourceIdentifiers, accessibles);
            printResults("vtConcatenateAndSliceSingleTable", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(3);
            final RowAccessible[] accessibles = VirtualTableExamples.dataConcatenateAndSlice();
            final VirtualTable table = VirtualTableExamples.vtConcatenateAndSliceFullSingleTable(sourceIdentifiers, accessibles);
            printResults("vtConcatenateAndSliceFullSingleTable", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(3);
            final RowAccessible[] accessibles = VirtualTableExamples.dataConcatenateAndSlice();
            final VirtualTable table = VirtualTableExamples.vtConcatenateAndSliceFullTable(sourceIdentifiers, accessibles);
            printResults("vtConcatenateAndSliceFullTable", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(2);
            final RowAccessible[] accessibles = VirtualTableExamples.dataAppendMissing();
            final VirtualTable table = VirtualTableExamples.vtAppendMissing(sourceIdentifiers, accessibles);
            printResults("vtAppendMissing", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(1);
            final RowAccessible[] accessibles = VirtualTableExamples.dataSimpleMap();
            final VirtualTable table = VirtualTableExamples.vtSimpleMap(sourceIdentifiers, accessibles);
            printResults("vtSimpleMap", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(1);
            final RowAccessible[] accessibles = VirtualTableExamples.dataSimpleRowFilter();
            final VirtualTable table = VirtualTableExamples.vtSimpleRowFilter(sourceIdentifiers, accessibles);
            printResults("vtSimpleRowFilter", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(1);
            final RowAccessible[] accessibles = VirtualTableExamples.dataConsecutiveRowFilters();
            final VirtualTable table = VirtualTableExamples.vtConsecutiveRowFilters(sourceIdentifiers, accessibles);
            printResults("vtConsecutiveRowFilters", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(1);
            final RowAccessible[] accessibles = VirtualTableExamples.dataMapsAndFilters();
            final VirtualTable table = VirtualTableExamples.vtMapsAndFilters(sourceIdentifiers, accessibles);
            printResults("vtMapsAndFilters", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(2);
            final RowAccessible[] accessibles = VirtualTableExamples.dataFiltersMapAndConcatenate();
            final VirtualTable table = VirtualTableExamples.vtFiltersMapAndConcatenate(sourceIdentifiers, accessibles);
            printResults("vtFiltersMapAndConcatenate", sourceIdentifiers, table, accessibles);
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

        final RagGraph graph = SpecGraphBuilder.buildSpecGraph(table);
        final List<RagNode> orderedRag = RagBuilder.createOrderedRag(graph);
        final CursorAssemblyPlan cursorAssemblyPlan = CapBuilder.createCursorAssemblyPlan(orderedRag);
        final ColumnarSchema schema = RagBuilder.createSchema(orderedRag);
        final RowAccessible rows = createRowAccessible(schema, cursorAssemblyPlan, uuidRowAccessibleMap);

        System.out.println(exampleName);
        System.out.println("------------------------");
        System.out.println("size = " + rows.size());
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
