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
import org.knime.core.table.virtual.graph.rag.RagGraphProperties;
import org.knime.core.table.virtual.graph.rag.RagNode;
import org.knime.core.table.virtual.graph.rag.SpecGraphBuilder;
import org.knime.core.table.virtual.graph.util.ReadAccessUtils;
import org.knime.core.table.virtual.spec.SourceTableProperties;
import org.knime.core.table.virtual.spec.SourceTableProperties.CursorType;

public class ExecCapAll {

    public static void main(final String[] args) {
        {
            final UUID[] sourceIdentifiers = createSourceIds(1);
            final RowAccessible[] accessibles = VirtualTableTests.dataMinimal();
            final VirtualTable table = VirtualTableTests.vtMinimal(sourceIdentifiers, accessibles);
            printResults("vtMinimal", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(1);
            final RowAccessible[] accessibles = VirtualTableTests.dataLinear();
            final VirtualTable table = VirtualTableTests.vtLinear(sourceIdentifiers, accessibles);
            printResults("vtLinear", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(1);
            final RowAccessible[] accessibles = VirtualTableTests.dataForkJoin();
            final VirtualTable table = VirtualTableTests.vtForkJoin(sourceIdentifiers, accessibles);
            printResults("vtForkJoin", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(1);
            final RowAccessible[] accessibles = toLookahead(VirtualTableTests.dataForkJoinLookALike());
            final VirtualTable table = VirtualTableTests.vtForkJoinLookALike(sourceIdentifiers, accessibles);
            printResults("vtForkJoinLookALike", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(2);
            final RowAccessible[] accessibles = VirtualTableTests.dataAppend();
            final VirtualTable table = VirtualTableTests.vtAppend(sourceIdentifiers, accessibles);
            printResults("vtAppend", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(2);
            final RowAccessible[] accessibles = VirtualTableTests.dataAppendAndFilterColumns();
            final VirtualTable table = VirtualTableTests.vtAppendAndFilterColumns(sourceIdentifiers, accessibles);
            printResults("vtAppendAndFilterColumns", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(2);
            final RowAccessible[] accessibles = VirtualTableTests.dataAppendAndSlice();
            final VirtualTable table = VirtualTableTests.vtAppendAndSlice(sourceIdentifiers, accessibles);
            printResults("vtAppendAndSlice", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(2);
            final RowAccessible[] accessibles = VirtualTableTests.dataAppendAndAppendMissing();
            final VirtualTable table = VirtualTableTests.vtAppendAndAppendMissing(sourceIdentifiers, accessibles);
            printResults("vtAppendAndAppendMissing", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(2);
            final RowAccessible[] accessibles = VirtualTableTests.dataConcatenate();
            final VirtualTable table = VirtualTableTests.vtConcatenate(sourceIdentifiers, accessibles);
            printResults("vtConcatenate", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(3);
            final RowAccessible[] accessibles = VirtualTableTests.dataConcatenateAndSlice();
            final VirtualTable table = VirtualTableTests.vtConcatenateAndSlice(sourceIdentifiers, accessibles);
            printResults("vtConcatenateAndSlice", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(3);
            final RowAccessible[] accessibles = VirtualTableTests.dataConcatenateAndSlice();
            final VirtualTable table = VirtualTableTests.vtConcatenateAndSliceSingleTable(sourceIdentifiers, accessibles);
            printResults("vtConcatenateAndSliceSingleTable", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(3);
            final RowAccessible[] accessibles = VirtualTableTests.dataConcatenateAndSlice();
            final VirtualTable table = VirtualTableTests.vtConcatenateAndSliceFullSingleTable(sourceIdentifiers, accessibles);
            printResults("vtConcatenateAndSliceFullSingleTable", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(3);
            final RowAccessible[] accessibles = VirtualTableTests.dataConcatenateAndSlice();
            final VirtualTable table = VirtualTableTests.vtConcatenateAndSliceFullTable(sourceIdentifiers, accessibles);
            printResults("vtConcatenateAndSliceFullTable", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(2);
            final RowAccessible[] accessibles = VirtualTableTests.dataAppendMissing();
            final VirtualTable table = VirtualTableTests.vtAppendMissing(sourceIdentifiers, accessibles);
            printResults("vtAppendMissing", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(1);
            final RowAccessible[] accessibles = VirtualTableTests.dataSimpleMap();
            final VirtualTable table = VirtualTableTests.vtSimpleMap(sourceIdentifiers, accessibles);
            printResults("vtSimpleMap", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(1);
            final RowAccessible[] accessibles = VirtualTableTests.dataSimpleRowFilter();
            final VirtualTable table = VirtualTableTests.vtSimpleRowFilter(sourceIdentifiers, accessibles);
            printResults("vtSimpleRowFilter", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(1);
            final RowAccessible[] accessibles = VirtualTableTests.dataConsecutiveRowFilters();
            final VirtualTable table = VirtualTableTests.vtConsecutiveRowFilters(sourceIdentifiers, accessibles);
            printResults("vtConsecutiveRowFilters", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(1);
            final RowAccessible[] accessibles = VirtualTableTests.dataMapsAndFilters();
            final VirtualTable table = VirtualTableTests.vtMapsAndFilters(sourceIdentifiers, accessibles);
            printResults("vtMapsAndFilters", sourceIdentifiers, table, accessibles);
        }

        {
            final UUID[] sourceIdentifiers = createSourceIds(2);
            final RowAccessible[] accessibles = VirtualTableTests.dataFiltersMapAndConcatenate();
            final VirtualTable table = VirtualTableTests.vtFiltersMapAndConcatenate(sourceIdentifiers, accessibles);
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
        final ColumnarSchema schema = RagBuilder.createSchema(orderedRag);
        final CursorType cursorType = RagGraphProperties.supportedCursorType(orderedRag);
        final RowAccessible rows = createRowAccessible(graph, schema, cursorType, uuidRowAccessibleMap);

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
