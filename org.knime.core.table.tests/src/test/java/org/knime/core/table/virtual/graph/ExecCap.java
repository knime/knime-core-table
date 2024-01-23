package org.knime.core.table.virtual.graph;

import static org.knime.core.table.RowAccessiblesTestUtils.toRowAccessible;
import static org.knime.core.table.virtual.graph.exec.CapExecutor.createRowAccessible;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;

import org.knime.core.table.RowAccessiblesTestUtils.TestRowAccessible;
import org.knime.core.table.RowAccessiblesTestUtils.TestRowWriteAccessible;
import org.knime.core.table.cursor.Cursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.row.RowWriteAccessible;
import org.knime.core.table.row.Selection;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.VirtualTable;
import org.knime.core.table.virtual.graph.exec.CapExecutor;
import org.knime.core.table.virtual.graph.rag.RagBuilder;
import org.knime.core.table.virtual.graph.rag.RagGraph;
import org.knime.core.table.virtual.graph.rag.RagGraphProperties;
import org.knime.core.table.virtual.graph.rag.RagNode;
import org.knime.core.table.virtual.graph.rag.SpecGraphBuilder;
import org.knime.core.table.virtual.graph.util.ReadAccessUtils;
import org.knime.core.table.virtual.spec.SourceTableProperties.CursorType;

public class ExecCap {

    public static void main(final String[] args) {

        boolean doExecute = false;
        UUID[] sinkIdentifiers = null;
        RowWriteAccessible[] sinkAccessibles = null;

//        final UUID[] sourceIdentifiers = createSourceIds(1);
//        final RowAccessible[] sourceAccessibles = VirtualTableTests.dataMinimal();
//        final VirtualTable table = VirtualTableTests.vtMinimal(sourceIdentifiers, sourceAccessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(2);
//        final RowAccessible[] sourceAccessibles = VirtualTableTests.dataAppend();
//        final VirtualTable table = VirtualTableTests.vtAppend(sourceIdentifiers, sourceAccessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(2);
//        final RowAccessible[] sourceAccessibles = VirtualTableTests.dataAppendAndSlice();
//        final VirtualTable table = VirtualTableTests.vtAppendAndSlice(sourceIdentifiers, sourceAccessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(1);
//        final RowAccessible[] sourceAccessibles = VirtualTableTests.dataForkJoin();
//        final VirtualTable table = VirtualTableTests.vtForkJoin(sourceIdentifiers, sourceAccessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(1);
//        final RowAccessible[] sourceAccessibles = toLookahead(VirtualTableTests.dataForkJoinLookALike());
//        final VirtualTable table = VirtualTableTests.vtForkJoinLookALike(sourceIdentifiers, sourceAccessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(2);
//        final RowAccessible[] sourceAccessibles = VirtualTableTests.dataConcatenate();
//        final VirtualTable table = VirtualTableTests.vtConcatenate(sourceIdentifiers, sourceAccessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(3);
//        final RowAccessible[] sourceAccessibles = VirtualTableTests.dataConcatenateAndSlice();
//        final VirtualTable table = VirtualTableTests.vtConcatenateAndSlice(sourceIdentifiers, sourceAccessibles);
//        final VirtualTable table = VirtualTableTests.vtConcatenateAndSliceSingleTable(sourceIdentifiers, sourceAccessibles);
//        final VirtualTable table = VirtualTableTests.vtConcatenateAndSliceFullSingleTable(sourceIdentifiers, sourceAccessibles);
//        final VirtualTable table = VirtualTableTests.vtConcatenateAndSliceFullTable(sourceIdentifiers, sourceAccessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(2);
//        final RowAccessible[] sourceAccessibles = VirtualTableTests.dataAppendMissing();
//        final VirtualTable table = VirtualTableTests.vtAppendMissing(sourceIdentifiers, sourceAccessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(1);
//        final RowAccessible[] sourceAccessibles = VirtualTableTests.dataSimpleMap();
//        final VirtualTable table = VirtualTableTests.vtSimpleMap(sourceIdentifiers, sourceAccessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(1);
//        final RowAccessible[] sourceAccessibles = VirtualTableTests.dataSimpleRowFilter();
//        final VirtualTable table = VirtualTableTests.vtSimpleRowFilter(sourceIdentifiers, sourceAccessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(1);
//        final RowAccessible[] sourceAccessibles = VirtualTableTests.dataConsecutiveRowFilters();
//        final VirtualTable table = VirtualTableTests.vtConsecutiveRowFilters(sourceIdentifiers, sourceAccessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(1);
//        final RowAccessible[] sourceAccessibles = VirtualTableTests.dataMapsAndFilters();
//        final VirtualTable table = VirtualTableTests.vtMapsAndFilters(sourceIdentifiers, sourceAccessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(2);
//        final RowAccessible[] sourceAccessibles = VirtualTableTests.dataFiltersMapAndConcatenate();
//        final VirtualTable table = VirtualTableTests.vtFiltersMapAndConcatenate(sourceIdentifiers, sourceAccessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(1);
//        final RowAccessible[] sourcesourceAccessibles = VirtualTableTests.dataMinimal();
//        sinkIdentifiers = createSourceIds(1);
//        sinkAccessibles = new RowWriteAccessible[]{RowsourceAccessiblesTestUtils.createRowWriteAccessible(ColumnarSchema.of(STRING))};
//        final VirtualTable table = VirtualTableTests.vtMaterializeMinimal(sourceIdentifiers, sourcesourceAccessibles, sinkIdentifiers, sinksourceAccessibles);
//        doExecute = true;

//        final UUID[] sourceIdentifiers = createSourceIds(1);
//        final RowAccessible[] sourceAccessibles = VirtualTableTests.dataMinimal();
//        final VirtualTable table = VirtualTableTests.vtRowIndexMap(sourceIdentifiers, sourceAccessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(1);
//        final RowAccessible[] sourceAccessibles = VirtualTableTests.dataMinimal();
//        final VirtualTable table = VirtualTableTests.vtRowIndexMapAndSlice(sourceIdentifiers, sourceAccessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(1);
//        final RowAccessible[] sourceAccessibles = VirtualTableTests.dataMinimal();
//        final VirtualTable table = VirtualTableTests.vtRowIndexMapsParallel(sourceIdentifiers, sourceAccessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(1);
//        final RowAccessible[] sourceAccessibles = VirtualTableTests.dataMinimal();
//        final VirtualTable table = VirtualTableTests.vtRowIndexMapsParallelAndSlice(sourceIdentifiers, sourceAccessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(1);
//        final RowAccessible[] sourceAccessibles = VirtualTableTests.dataMinimal();
//        final VirtualTable table = VirtualTableTests.vtRowIndexMapsSequential(sourceIdentifiers, sourceAccessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(2);
//        final RowAccessible[] sourceAccessibles = VirtualTableTests.dataObserve();
//        final VirtualTable table = VirtualTableTests.vtObserve(sourceIdentifiers, sourceAccessibles);

        final UUID[] sourceIdentifiers = createSourceIds(1);
        final RowAccessible[] sourceAccessibles = VirtualTableTests.dataSimpleMapWithExpression();
        final VirtualTable table = VirtualTableTests.vtSimpleMapWithExpression(sourceIdentifiers, sourceAccessibles);


        if ( doExecute ) {

            // ----------------------------------------------
            // execute

            final Map<UUID, RowAccessible> uuidRowAccessibleMap = new HashMap<>();
            for (int i = 0; i < sourceIdentifiers.length; ++i) {
                uuidRowAccessibleMap.put(sourceIdentifiers[i], sourceAccessibles[i]);
            }

            final Map<UUID, RowWriteAccessible> uuidRowWriteAccessibleMap = new HashMap<>();
            for (int i = 0; i < sinkIdentifiers.length; ++i) {
                uuidRowWriteAccessibleMap.put(sinkIdentifiers[i], sinkAccessibles[i]);
            }

            final RagGraph specGraph = SpecGraphBuilder.buildSpecGraph(table);

            try {
                CapExecutor.execute(specGraph, uuidRowAccessibleMap, uuidRowWriteAccessibleMap);
            } catch (CancellationException | CompletionException e) {
                e.printStackTrace();
            }

            TestRowAccessible rows =
                    toRowAccessible((TestRowWriteAccessible)sinkAccessibles[0]);
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
        } else {

            // ----------------------------------------------
            // create accessible

            final Map<UUID, RowAccessible> uuidRowAccessibleMap = new HashMap<>();
            for (int i = 0; i < sourceIdentifiers.length; ++i) {
                uuidRowAccessibleMap.put(sourceIdentifiers[i], sourceAccessibles[i]);
            }

            final RagGraph graph = SpecGraphBuilder.buildSpecGraph(table);
            final List<RagNode> orderedRag = RagBuilder.createOrderedRag(graph);
            final ColumnarSchema schema = RagBuilder.createSchema(orderedRag);
            final CursorType cursorType = RagGraphProperties.supportedCursorType(orderedRag);
            final RowAccessible rows = createRowAccessible(graph, schema, cursorType, uuidRowAccessibleMap);

            System.out.println("supported CursorType = " + RagGraphProperties.supportedCursorType(orderedRag));

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

            try (final Cursor<ReadAccessRow> cursor = rows.createCursor(Selection.all().retainColumns(1))) {
                while (cursor.forward()) {
                    System.out.print("a = ");
                    for (int i = 0; i < cursor.access().size(); i++)
                        System.out.print(ReadAccessUtils.toString(cursor.access().getAccess(i)) + ", ");
                    System.out.println();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            try (final Cursor<ReadAccessRow> cursor = rows.createCursor(Selection.all().retainColumns(0))) {
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
    }

    private static UUID[] createSourceIds(final int n) {
        final UUID[] ids = new UUID[n];
        Arrays.setAll(ids, i -> UUID.randomUUID());
        return ids;
    }
}
