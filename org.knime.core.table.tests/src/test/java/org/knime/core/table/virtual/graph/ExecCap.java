package org.knime.core.table.virtual.graph;

import static org.knime.core.table.RowAccessiblesTestUtils.toRowAccessible;
import static org.knime.core.table.schema.DataSpecs.STRING;
import static org.knime.core.table.virtual.graph.exec.CapExecutor.createRowAccessible;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;

import org.knime.core.table.RowAccessiblesTestUtils;
import org.knime.core.table.RowAccessiblesTestUtils.TestRowAccessible;
import org.knime.core.table.RowAccessiblesTestUtils.TestRowWriteAccessible;
import org.knime.core.table.cursor.Cursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.row.RowWriteAccessible;
import org.knime.core.table.row.Selection;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.VirtualTable;
import org.knime.core.table.virtual.graph.cap.CapBuilder;
import org.knime.core.table.virtual.graph.cap.CursorAssemblyPlan;
import org.knime.core.table.virtual.graph.exec.CapExecutor;
import org.knime.core.table.virtual.graph.rag.RagBuilder;
import org.knime.core.table.virtual.graph.rag.RagGraph;
import org.knime.core.table.virtual.graph.rag.RagGraphProperties;
import org.knime.core.table.virtual.graph.rag.RagNode;
import org.knime.core.table.virtual.graph.rag.SpecGraphBuilder;
import org.knime.core.table.virtual.graph.util.ReadAccessUtils;
import org.knime.core.table.virtual.spec.SourceTableProperties;
import org.knime.core.table.virtual.spec.SourceTableProperties.CursorType;

public class ExecCap {

    public static void main(final String[] args) {

        boolean doExecute = false;
        UUID[] sinkIdentifiers = null;
        RowWriteAccessible[] sinkAccessibles = null;

//        final UUID[] sourceIdentifiers = createSourceIds(1);
//        final RowAccessible[] sourceAccessibles = VirtualTableExamples.dataMinimal();
//        final VirtualTable table = VirtualTableExamples.vtMinimal(sourceIdentifiers, sourceAccessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(2);
//        final RowAccessible[] sourceAccessibles = VirtualTableExamples.dataAppend();
//        final VirtualTable table = VirtualTableExamples.vtAppend(sourceIdentifiers, sourceAccessibles);

        final UUID[] sourceIdentifiers = createSourceIds(2);
        final RowAccessible[] sourceAccessibles = VirtualTableExamples.dataAppendAndSlice();
        final VirtualTable table = VirtualTableExamples.vtAppendAndSlice(sourceIdentifiers, sourceAccessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(1);
//        final RowAccessible[] sourceAccessibles = VirtualTableExamples.dataForkJoin();
//        final VirtualTable table = VirtualTableExamples.vtForkJoin(sourceIdentifiers, sourceAccessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(1);
//        final RowAccessible[] sourceAccessibles = toLookahead(VirtualTableExamples.dataForkJoinLookALike());
//        final VirtualTable table = VirtualTableExamples.vtForkJoinLookALike(sourceIdentifiers, sourceAccessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(2);
//        final RowAccessible[] sourceAccessibles = VirtualTableExamples.dataConcatenate();
//        final VirtualTable table = VirtualTableExamples.vtConcatenate(sourceIdentifiers, sourceAccessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(3);
//        final RowAccessible[] sourceAccessibles = VirtualTableExamples.dataConcatenateAndSlice();
//        final VirtualTable table = VirtualTableExamples.vtConcatenateAndSlice(sourceIdentifiers, sourceAccessibles);
//        final VirtualTable table = VirtualTableExamples.vtConcatenateAndSliceSingleTable(sourceIdentifiers, sourceAccessibles);
//        final VirtualTable table = VirtualTableExamples.vtConcatenateAndSliceFullSingleTable(sourceIdentifiers, sourceAccessibles);
//        final VirtualTable table = VirtualTableExamples.vtConcatenateAndSliceFullTable(sourceIdentifiers, sourceAccessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(2);
//        final RowAccessible[] sourceAccessibles = VirtualTableExamples.dataAppendMissing();
//        final VirtualTable table = VirtualTableExamples.vtAppendMissing(sourceIdentifiers, sourceAccessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(1);
//        final RowAccessible[] sourceAccessibles = VirtualTableExamples.dataSimpleMap();
//        final VirtualTable table = VirtualTableExamples.vtSimpleMap(sourceIdentifiers, sourceAccessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(1);
//        final RowAccessible[] sourceAccessibles = VirtualTableExamples.dataSimpleRowFilter();
//        final VirtualTable table = VirtualTableExamples.vtSimpleRowFilter(sourceIdentifiers, sourceAccessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(1);
//        final RowAccessible[] sourceAccessibles = VirtualTableExamples.dataConsecutiveRowFilters();
//        final VirtualTable table = VirtualTableExamples.vtConsecutiveRowFilters(sourceIdentifiers, sourceAccessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(1);
//        final RowAccessible[] sourceAccessibles = VirtualTableExamples.dataMapsAndFilters();
//        final VirtualTable table = VirtualTableExamples.vtMapsAndFilters(sourceIdentifiers, sourceAccessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(2);
//        final RowAccessible[] sourceAccessibles = VirtualTableExamples.dataFiltersMapAndConcatenate();
//        final VirtualTable table = VirtualTableExamples.vtFiltersMapAndConcatenate(sourceIdentifiers, sourceAccessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(1);
//        final RowAccessible[] sourcesourceAccessibles = VirtualTableExamples.dataMinimal();
//        sinkIdentifiers = createSourceIds(1);
//        sinkAccessibles = new RowWriteAccessible[]{RowsourceAccessiblesTestUtils.createRowWriteAccessible(ColumnarSchema.of(STRING))};
//        final VirtualTable table = VirtualTableExamples.vtMaterializeMinimal(sourceIdentifiers, sourcesourceAccessibles, sinkIdentifiers, sinksourceAccessibles);
//        doExecute = true;

//        final UUID[] sourceIdentifiers = createSourceIds(1);
//        final RowAccessible[] sourceAccessibles = VirtualTableExamples.dataMinimal();
//        final VirtualTable table = VirtualTableExamples.vtRowIndexMap(sourceIdentifiers, sourceAccessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(1);
//        final RowAccessible[] sourceAccessibles = VirtualTableExamples.dataMinimal();
//        final VirtualTable table = VirtualTableExamples.vtRowIndexMapAndSlice(sourceIdentifiers, sourceAccessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(1);
//        final RowAccessible[] sourceAccessibles = VirtualTableExamples.dataMinimal();
//        final VirtualTable table = VirtualTableExamples.vtRowIndexMapsParallel(sourceIdentifiers, sourceAccessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(1);
//        final RowAccessible[] sourceAccessibles = VirtualTableExamples.dataMinimal();
//        final VirtualTable table = VirtualTableExamples.vtRowIndexMapsParallelAndSlice(sourceIdentifiers, sourceAccessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(1);
//        final RowAccessible[] sourceAccessibles = VirtualTableExamples.dataMinimal();
//        final VirtualTable table = VirtualTableExamples.vtRowIndexMapsSequential(sourceIdentifiers, sourceAccessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(2);
//        final RowAccessible[] sourceAccessibles = VirtualTableExamples.dataObserve();
//        final VirtualTable table = VirtualTableExamples.vtObserve(sourceIdentifiers, sourceAccessibles);

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
