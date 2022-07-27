package org.knime.core.table.virtual.graph;

import static java.util.UUID.randomUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.knime.core.table.RowAccessiblesTestUtils.assertCanForwardPredictsForward;
import static org.knime.core.table.RowAccessiblesTestUtils.assertTableEqualsValues;
import static org.knime.core.table.RowAccessiblesTestUtils.toLookahead;
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
import java.util.function.BiFunction;
import java.util.function.Supplier;

import org.junit.Test;
import org.knime.core.table.RowAccessiblesTestUtils;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.LookaheadRowAccessible;
import org.knime.core.table.virtual.VirtualTable;
import org.knime.core.table.virtual.graph.cap.CapBuilder;
import org.knime.core.table.virtual.graph.cap.CursorAssemblyPlan;
import org.knime.core.table.virtual.graph.rag.RagBuilder;
import org.knime.core.table.virtual.graph.rag.RagNode;
import org.knime.core.table.virtual.spec.MapTransformSpec.MapperFactory;
import org.knime.core.table.virtual.spec.RowFilterTransformSpec.RowFilterFactory;
import org.knime.core.table.virtual.spec.SourceTableProperties;

public class VirtualTableExamples {

    private static void testTransformedTable(
            final ColumnarSchema expectedSchema,
            final Object[][] expectedValues,
            final int expectedNumRows,
            final Supplier<RowAccessible[]> sourcesSupplier,
            final BiFunction<UUID[], RowAccessible[], VirtualTable> virtualTableSupplier) {

        final RowAccessible[] sources = sourcesSupplier.get();
        final UUID[] sourceIds = new UUID[sources.length];
        Arrays.setAll(sourceIds, i -> randomUUID());
        final VirtualTable table = virtualTableSupplier.apply(sourceIds, sources);

        final List<RagNode> rag = RagBuilder.createOrderedRag(table);
        final CursorAssemblyPlan cap = CapBuilder.createCursorAssemblyPlan(rag);
        final ColumnarSchema schema = RagBuilder.createSchema(rag);

        final Map<UUID, RowAccessible> sourceMap = new HashMap<>();
        for (int i = 0; i < sourceIds.length; ++i) {
            sourceMap.put(sourceIds[i], sources[i]);
        }
        final RowAccessible rowAccessible = createRowAccessible(schema, cap, sourceMap);

        if ( expectedNumRows < 0 ) {
            assertTrue(rowAccessible.size() < 0);
        } else {
            assertEquals(expectedNumRows, rowAccessible.size());
        }
        assertEquals(expectedSchema, table.getSchema());
        assertTableEqualsValues(expectedValues, rowAccessible, false);
        assertTableEqualsValues(expectedValues, rowAccessible, true);
    }

    private static void testTransformedTableLookahead(
            final boolean expectedLookahead,
            final Supplier<RowAccessible[]> sourcesSupplier,
            final BiFunction<UUID[], RowAccessible[], VirtualTable> virtualTableSupplier) {

        final RowAccessible[] sources = toLookahead(sourcesSupplier.get());
        final UUID[] sourceIds = new UUID[sources.length];
        Arrays.setAll(sourceIds, i -> randomUUID());
        final VirtualTable table = virtualTableSupplier.apply(sourceIds, sources);

        final List<RagNode> rag = RagBuilder.createOrderedRag(table);
        final CursorAssemblyPlan cap = CapBuilder.createCursorAssemblyPlan(rag);
        final ColumnarSchema schema = RagBuilder.createSchema(rag);

        final Map<UUID, RowAccessible> sourceMap = new HashMap<>();
        for (int i = 0; i < sourceIds.length; ++i) {
            sourceMap.put(sourceIds[i], sources[i]);
        }
        final RowAccessible rowAccessible = createRowAccessible(schema, cap, sourceMap);
        final boolean lookahead = rowAccessible instanceof LookaheadRowAccessible;

        assertEquals(expectedLookahead, lookahead);
        if (lookahead) {
            assertCanForwardPredictsForward(rowAccessible);
        }
    }



    public static VirtualTable vtMinimal(final UUID[] sourceIdentifiers, final RowAccessible[] sources) {
        final ColumnarSchema schema = ColumnarSchema.of(DOUBLE, INT, STRING);
        return new VirtualTable(sourceIdentifiers[0], new SourceTableProperties(sources[0])).permute(0, 2, 1).filterColumns(1);
    }

    public static VirtualTable vtMinimal() {
        return vtMinimal(new UUID[]{randomUUID()}, dataMinimal());
    }

    public static RowAccessible[] dataMinimal() {
        final ColumnarSchema schema = ColumnarSchema.of(DOUBLE, INT, STRING);
        final Object[][] values = new Object[][]{ //
                new Object[]{0.1, 1, "First"}, //
                new Object[]{0.2, 2, "Second"}, //
                new Object[]{0.3, 3, "Third"}, //
                new Object[]{0.4, 4, "Fourth"}, //
                new Object[]{0.5, 5, "Fifth"} //
        };
        return new RowAccessible[]{RowAccessiblesTestUtils.createRowAccessibleFromRowWiseValues(schema, values)};
    }

    @Test
    public void testMinimal() throws IOException {
        final ColumnarSchema expectedSchema = ColumnarSchema.of(STRING);
        final Object[][] expectedValues = new Object[][]{ //
                new Object[]{"First"}, //
                new Object[]{"Second"}, //
                new Object[]{"Third"}, //
                new Object[]{"Fourth"}, //
                new Object[]{"Fifth"} //
        };
        testTransformedTable(expectedSchema, expectedValues, expectedValues.length, VirtualTableExamples::dataMinimal, VirtualTableExamples::vtMinimal);
        testTransformedTableLookahead(true, VirtualTableExamples::dataMinimal, VirtualTableExamples::vtMinimal);
    }



    public static VirtualTable vtLinear(final UUID[] sourceIdentifiers, final RowAccessible[] sources) {
        final ColumnarSchema schema = ColumnarSchema.of(DOUBLE, INT, STRING);
        return new VirtualTable(sourceIdentifiers[0], new SourceTableProperties(sources[0])).slice(1, 6).filterColumns(1);
    }

    public static VirtualTable vtLinear() {
        return vtLinear(new UUID[]{randomUUID()}, dataLinear());
    }

    public static RowAccessible[] dataLinear() {
        return dataMinimal();
    }

    @Test
    public void testLinear() {
        final ColumnarSchema expectedSchema = ColumnarSchema.of(INT);
        final Object[][] expectedValues = new Object[][]{ //
                new Object[]{2}, //
                new Object[]{3}, //
                new Object[]{4}, //
                new Object[]{5}, //
        };
        testTransformedTable(expectedSchema, expectedValues, expectedValues.length, VirtualTableExamples::dataLinear, VirtualTableExamples::vtLinear);
        testTransformedTableLookahead(true, VirtualTableExamples::dataLinear, VirtualTableExamples::vtLinear);
    }



    public static VirtualTable vtForkJoin(final UUID[] sourceIdentifiers, final RowAccessible[] sources) {
        final VirtualTable transformedTable = new VirtualTable(sourceIdentifiers[0], new SourceTableProperties(sources[0])).slice(1, 6);
        final VirtualTable forkedTable1 = transformedTable.filterColumns(1).appendMissingValueColumns(List.of(DOUBLE.spec()), List.of(DOUBLE.traits()));
        final VirtualTable forkedTable2 = transformedTable.permute(2, 1, 0);
        final VirtualTable joinedTransformedTable = forkedTable1.append(Arrays.asList(forkedTable2));
        return joinedTransformedTable;
    }

    public static VirtualTable vtForkJoin() {
        return vtForkJoin(new UUID[]{randomUUID()}, dataForkJoin());
    }

    public static RowAccessible[] dataForkJoin() {
        final ColumnarSchema schema = ColumnarSchema.of(DOUBLE, INT, STRING);
        final Object[][] values = new Object[][]{ //
                new Object[]{0.1, 1, "First"}, //
                new Object[]{0.2, 2, "Second"}, //
                new Object[]{0.3, 3, "Third"}, //
                new Object[]{0.4, 4, "Fourth"}, //
                new Object[]{0.5, 5, "Fifth"}, //
                new Object[]{0.6, 6, "Sixth"}, //
                new Object[]{0.7, 7, "Seventh"} //
        };
        return new RowAccessible[]{RowAccessiblesTestUtils.createRowAccessibleFromRowWiseValues(schema, values)};
    }

    @Test
    public void testForkJoin() {
        final ColumnarSchema expectedSchema = ColumnarSchema.of(INT, DOUBLE, STRING, INT, DOUBLE);
        final Object[][] expectedValues = new Object[][]{ //
                new Object[]{2, null, "Second", 2, 0.2}, //
                new Object[]{3, null, "Third",  3, 0.3}, //
                new Object[]{4, null, "Fourth", 4, 0.4}, //
                new Object[]{5, null, "Fifth",  5, 0.5}, //
                new Object[]{6, null, "Sixth",  6, 0.6}, //
        };
        testTransformedTable(expectedSchema, expectedValues, expectedValues.length, VirtualTableExamples::dataForkJoin, VirtualTableExamples::vtForkJoin);
        testTransformedTableLookahead(true, VirtualTableExamples::dataForkJoin, VirtualTableExamples::vtForkJoin);
    }



    public static VirtualTable vtForkJoinLookALike(final UUID[] sourceIdentifiers, final RowAccessible[] sources) {
        final VirtualTable transformedTable = new VirtualTable(sourceIdentifiers[0], new SourceTableProperties(sources[0])).slice(1, 6);
        final VirtualTable forkedTable1 = transformedTable.filterColumns(1).appendMissingValueColumns(List.of(DOUBLE.spec()), List.of(DOUBLE.traits()));
        final VirtualTable forkedTable2 = transformedTable.slice(2,3).permute(2, 1, 0);
        final VirtualTable joinedTransformedTable = forkedTable1.append(Arrays.asList(forkedTable2));
        return joinedTransformedTable;
    }

    public static VirtualTable vtForkJoinLookALike() {
        return vtForkJoinLookALike(new UUID[]{randomUUID()}, dataForkJoinLookALike());
    }

    public static RowAccessible[] dataForkJoinLookALike() {
        return dataForkJoin();
    }

    @Test
    public void testForkJoinLookALike() {
        final ColumnarSchema expectedSchema = ColumnarSchema.of(INT, DOUBLE, STRING, INT, DOUBLE);
        final Object[][] expectedValues = new Object[][]{ //
                new Object[]{2, null, "Fourth", 4, 0.4}, //
                new Object[]{3, null, null, null, null}, //
                new Object[]{4, null, null, null, null}, //
                new Object[]{5, null, null, null, null}, //
                new Object[]{6, null, null, null, null}, //
        };
        testTransformedTable(expectedSchema, expectedValues, expectedValues.length, VirtualTableExamples::dataForkJoinLookALike, VirtualTableExamples::vtForkJoinLookALike);
        testTransformedTableLookahead(true, VirtualTableExamples::dataForkJoinLookALike, VirtualTableExamples::vtForkJoinLookALike);
    }



    public static VirtualTable vtAppend(final UUID[] sourceIdentifiers, final RowAccessible[] sources) {
        final VirtualTable transformedTable2 = new VirtualTable(sourceIdentifiers[1], new SourceTableProperties(sources[1])).permute(1, 0);
        return new VirtualTable(sourceIdentifiers[0], new SourceTableProperties(sources[0])).filterColumns(1, 2).append(List.of(transformedTable2))
                .permute(1, 0, 2, 3).filterColumns(1, 2);
    }

    public static VirtualTable vtAppend() {
        return vtAppend(new UUID[]{randomUUID(), randomUUID()}, dataAppend());
    }

    public static RowAccessible[] dataAppend() {
        final ColumnarSchema schema1 = ColumnarSchema.of(DOUBLE, INT, STRING);
        final Object[][] values1 = new Object[][]{ //
                new Object[]{0.1, 1, "First"}, //
                new Object[]{0.2, 2, "Second"}, //
                new Object[]{0.3, 3, "Third"}, //
                new Object[]{0.4, 4, "Fourth"}, //
                new Object[]{0.5, 5, "Fifth"} //
        };
        final ColumnarSchema schema2 = ColumnarSchema.of(INT, DOUBLE);
        final Object[][] values2 = new Object[][]{ //
                new Object[]{11, 1.1}, //
                new Object[]{12, 1.2}, //
                new Object[]{13, 1.3}, //
                new Object[]{14, 1.4}, //
                new Object[]{15, 1.5} //
        };
        return new RowAccessible[] {
                RowAccessiblesTestUtils.createRowAccessibleFromRowWiseValues(schema1, values1),
                RowAccessiblesTestUtils.createRowAccessibleFromRowWiseValues(schema2, values2),
        };
    }

    @Test
    public void testAppend() {
        final ColumnarSchema expectedSchema = ColumnarSchema.of(INT, DOUBLE);
        final Object[][] expectedValues = new Object[][]{ //
                new Object[]{1, 1.1}, //
                new Object[]{2, 1.2}, //
                new Object[]{3, 1.3}, //
                new Object[]{4, 1.4}, //
                new Object[]{5, 1.5} //
        };
        testTransformedTable(expectedSchema, expectedValues, expectedValues.length, VirtualTableExamples::dataAppend, VirtualTableExamples::vtAppend);
        testTransformedTableLookahead(true, VirtualTableExamples::dataAppend, VirtualTableExamples::vtAppend);
    }



    public static VirtualTable vtAppendAndFilterColumns(final UUID[] sourceIdentifiers, final RowAccessible[] sources) {
        final VirtualTable transformedTable2 = new VirtualTable(sourceIdentifiers[1], new SourceTableProperties(sources[1])).permute(1, 0);
        return new VirtualTable(sourceIdentifiers[0], new SourceTableProperties(sources[0])).filterColumns(1, 2).append(List.of(transformedTable2))
                .permute(1, 0, 2, 3).filterColumns(3);
    }

    public static VirtualTable vtAppendAndFilterColumns() {
        return vtAppendAndFilterColumns(new UUID[]{randomUUID(), randomUUID()}, dataAppendAndFilterColumns());
    }

    public static RowAccessible[] dataAppendAndFilterColumns() {
        final ColumnarSchema schema1 = ColumnarSchema.of(DOUBLE, INT, STRING);
        final Object[][] values1 = new Object[][]{ //
                new Object[]{0.1, 1, "First"}, //
                new Object[]{0.2, 2, "Second"}, //
                new Object[]{0.3, 3, "Third"}, //
                new Object[]{0.4, 4, "Fourth"}, //
                new Object[]{0.5, 5, "Fifth"} //
        };
        final ColumnarSchema schema2 = ColumnarSchema.of(INT, DOUBLE);
        final Object[][] values2 = new Object[][]{ //
                new Object[]{11, 1.1}, //
                new Object[]{12, 1.2}, //
                new Object[]{13, 1.3}, //
        };
        return new RowAccessible[] {
                RowAccessiblesTestUtils.createRowAccessibleFromRowWiseValues(schema1, values1),
                RowAccessiblesTestUtils.createRowAccessibleFromRowWiseValues(schema2, values2),
        };
    }

    @Test
    public void testAppendAndFilterColumns() {
        final ColumnarSchema expectedSchema = ColumnarSchema.of(INT);
        final Object[][] expectedValues = new Object[][]{ //
                new Object[]{11}, //
                new Object[]{12}, //
                new Object[]{13}, //
                new Object[]{null}, // (missing)
                new Object[]{null}, // (missing)
        };
        testTransformedTable(expectedSchema, expectedValues, expectedValues.length, VirtualTableExamples::dataAppendAndFilterColumns, VirtualTableExamples::vtAppendAndFilterColumns);
        testTransformedTableLookahead(true, VirtualTableExamples::dataAppendAndFilterColumns, VirtualTableExamples::vtAppendAndFilterColumns);
    }



    public static VirtualTable vtAppendAndSlice(final UUID[] sourceIdentifiers, final RowAccessible[] sources) {
        final VirtualTable transformedTable2 = new VirtualTable(sourceIdentifiers[1], new SourceTableProperties(sources[1])).permute(1, 0);
        return new VirtualTable(sourceIdentifiers[0], new SourceTableProperties(sources[0])).filterColumns(1, 2).append(List.of(transformedTable2))
                .permute(1, 0, 2, 3).filterColumns(1, 2).slice(1, 4);
    }

    public static VirtualTable vtAppendAndSlice() {
        return vtAppendAndSlice(new UUID[]{randomUUID(), randomUUID()}, dataAppendAndSlice());
    }

    public static RowAccessible[] dataAppendAndSlice() {
        return dataAppend();
    }

    @Test
    public void testAppendAndSlice() {
        final ColumnarSchema expectedSchema = ColumnarSchema.of(INT, DOUBLE);
        final Object[][] expectedValues = new Object[][]{ //
                new Object[]{2, 1.2}, //
                new Object[]{3, 1.3}, //
                new Object[]{4, 1.4} //
        };
        testTransformedTable(expectedSchema, expectedValues, expectedValues.length, VirtualTableExamples::dataAppendAndSlice, VirtualTableExamples::vtAppendAndSlice);
        testTransformedTableLookahead(true, VirtualTableExamples::dataAppendAndSlice, VirtualTableExamples::vtAppendAndSlice);
    }



    public static VirtualTable vtAppendAndAppendMissing(final UUID[] sourceIdentifiers, final RowAccessible[] sources) {
        final VirtualTable transformedTable2 = new VirtualTable(sourceIdentifiers[1], new SourceTableProperties(sources[1]))
                .permute(1, 0)
                .appendMissingValueColumns(List.of(DOUBLE.spec()), List.of(DOUBLE.traits()));
        return new VirtualTable(sourceIdentifiers[0], new SourceTableProperties(sources[0]))
                .filterColumns(1, 2)
                .append(List.of(transformedTable2))
                .permute(1, 0, 2, 3, 4).filterColumns(1, 2, 4);
    }

    public static VirtualTable vtAppendAndAppendMissing() {
        return vtAppendAndAppendMissing(new UUID[]{randomUUID(), randomUUID()}, dataAppendAndAppendMissing());
    }

    public static RowAccessible[] dataAppendAndAppendMissing() {
        return dataAppend();
    }

    @Test
    public void testAppendAndAppendMissing() {
        final ColumnarSchema expectedSchema = ColumnarSchema.of(INT, DOUBLE, DOUBLE);
        final Object[][] expectedValues = new Object[][]{ //
                new Object[]{1, 1.1, null}, //
                new Object[]{2, 1.2, null}, //
                new Object[]{3, 1.3, null}, //
                new Object[]{4, 1.4, null}, //
                new Object[]{5, 1.5, null} //
        };
        testTransformedTable(expectedSchema, expectedValues, expectedValues.length, VirtualTableExamples::dataAppendAndAppendMissing, VirtualTableExamples::vtAppendAndAppendMissing);
        testTransformedTableLookahead(true, VirtualTableExamples::dataAppendAndAppendMissing, VirtualTableExamples::vtAppendAndAppendMissing);
    }



    public static VirtualTable vtConcatenate(final UUID[] sourceIdentifiers, final RowAccessible[] sources) {
        final VirtualTable transformedTable2 = new VirtualTable(sourceIdentifiers[1], new SourceTableProperties(sources[1])).permute(1, 0);
        return new VirtualTable(sourceIdentifiers[0], new SourceTableProperties(sources[0])).filterColumns(0,1).concatenate(List.of(transformedTable2)).filterColumns(1);
    }

    public static VirtualTable vtConcatenate() {
        return vtConcatenate(new UUID[]{randomUUID(), randomUUID()}, dataConcatenate());
    }

    public static RowAccessible[] dataConcatenate() {
        final ColumnarSchema schema1 = ColumnarSchema.of(DOUBLE, INT, STRING);
        final Object[][] values1 = new Object[][]{ //
                new Object[]{0.1, 1, "First"}, //
                new Object[]{0.2, 2, "Second"}, //
                new Object[]{0.3, 3, "Third"}, //
                new Object[]{0.4, 4, "Fourth"}, //
                new Object[]{0.5, 5, "Fifth"} //
        };
        final ColumnarSchema schema2 = ColumnarSchema.of(INT, DOUBLE);
        final Object[][] values2 = new Object[][]{ //
                new Object[]{11, 1.1}, //
                new Object[]{12, 1.2} //
        };
        return new RowAccessible[] {
                RowAccessiblesTestUtils.createRowAccessibleFromRowWiseValues(schema1, values1),
                RowAccessiblesTestUtils.createRowAccessibleFromRowWiseValues(schema2, values2),
        };
    }

    @Test
    public void testConcatenate() {
        final ColumnarSchema expectedSchema = ColumnarSchema.of(INT);
        final Object[][] expectedValues = new Object[][]{ //
                new Object[]{1}, //
                new Object[]{2}, //
                new Object[]{3}, //
                new Object[]{4}, //
                new Object[]{5}, //
                new Object[]{11}, //
                new Object[]{12} //
        };
        testTransformedTable(expectedSchema, expectedValues, expectedValues.length, VirtualTableExamples::dataConcatenate, VirtualTableExamples::vtConcatenate);
        testTransformedTableLookahead(true, VirtualTableExamples::dataConcatenate, VirtualTableExamples::vtConcatenate);
    }



    public static VirtualTable vtConcatenateAndSlice(final UUID[] sourceIdentifiers, final RowAccessible[] sources, final long sliceFrom, final long sliceTo) {
        final VirtualTable transformedTable2 = new VirtualTable(sourceIdentifiers[1], new SourceTableProperties(sources[1])).permute(1, 0);
        final VirtualTable transformedTable3 = new VirtualTable(sourceIdentifiers[2], new SourceTableProperties(sources[2])).permute(1, 0);
        return new VirtualTable(sourceIdentifiers[0], new SourceTableProperties(sources[0])).filterColumns(0,1).concatenate(List.of(transformedTable2, transformedTable3)).filterColumns(1).slice(sliceFrom, sliceTo);
    }

    public static VirtualTable vtConcatenateAndSlice(final UUID[] sourceIdentifiers, final RowAccessible[] sources) {
        return vtConcatenateAndSlice(sourceIdentifiers, sources, 6, 10);
    }

    public static VirtualTable vtConcatenateAndSliceSingleTable(final UUID[] sourceIdentifiers, final RowAccessible[] sources) {
        return vtConcatenateAndSlice(sourceIdentifiers, sources, 6, 8);
    }

    public static VirtualTable vtConcatenateAndSliceFullSingleTable(final UUID[] sourceIdentifiers, final RowAccessible[] sources) {
        return vtConcatenateAndSlice(sourceIdentifiers, sources, 5, 9);
    }

    public static VirtualTable vtConcatenateAndSliceFullTable(final UUID[] sourceIdentifiers, final RowAccessible[] sources) {
        return vtConcatenateAndSlice(sourceIdentifiers, sources, 5, 10);
    }

    public static VirtualTable vtConcatenateAndSlice() {
        return vtConcatenateAndSlice(new UUID[]{randomUUID(), randomUUID(), randomUUID()}, dataConcatenateAndSlice());
    }

    public static VirtualTable vtConcatenateAndSliceSingleTable() {
        return vtConcatenateAndSliceSingleTable(new UUID[]{randomUUID(), randomUUID(), randomUUID()}, dataConcatenateAndSlice());
    }

    public static VirtualTable vtConcatenateAndSliceFullSingleTable() {
        return vtConcatenateAndSliceFullSingleTable(new UUID[]{randomUUID(), randomUUID(), randomUUID()}, dataConcatenateAndSlice());
    }

    public static VirtualTable vtConcatenateAndSliceFullTable() {
        return vtConcatenateAndSliceFullTable(new UUID[]{randomUUID(), randomUUID(), randomUUID()}, dataConcatenateAndSlice());
    }

    public static RowAccessible[] dataConcatenateAndSlice() {
        final ColumnarSchema schema1 = ColumnarSchema.of(DOUBLE, INT, STRING);
        final Object[][] values1 = new Object[][]{ //
                new Object[]{0.1, 1, "First"}, //
                new Object[]{0.2, 2, "Second"}, //
                new Object[]{0.3, 3, "Third"}, //
                new Object[]{0.4, 4, "Fourth"}, //
                new Object[]{0.5, 5, "Fifth"} //
        };
        final ColumnarSchema schema2 = ColumnarSchema.of(INT, DOUBLE);
        final Object[][] values2 = new Object[][]{ //
                new Object[]{11, 1.1}, //
                new Object[]{12, 1.2}, //
                new Object[]{13, 1.3}, //
                new Object[]{14, 1.4}, //
        };
        final ColumnarSchema schema3 = ColumnarSchema.of(INT, DOUBLE);
        final Object[][] values3 = new Object[][]{ //
                new Object[]{16, 1.6}, //
                new Object[]{17, 1.7}, //
                new Object[]{18, 1.8}, //
        };
        return new RowAccessible[] {
                RowAccessiblesTestUtils.createRowAccessibleFromRowWiseValues(schema1, values1),
                RowAccessiblesTestUtils.createRowAccessibleFromRowWiseValues(schema2, values2),
                RowAccessiblesTestUtils.createRowAccessibleFromRowWiseValues(schema3, values3),
        };
    }

    @Test
    public void testConcatenateAndSlice() {
        final ColumnarSchema expectedSchema = ColumnarSchema.of(INT);
        final Object[][] expectedValues = new Object[][]{ //
                new Object[]{12}, //
                new Object[]{13}, //
                new Object[]{14}, //
                new Object[]{16}, //
        };
        testTransformedTable(expectedSchema, expectedValues, expectedValues.length, VirtualTableExamples::dataConcatenateAndSlice, VirtualTableExamples::vtConcatenateAndSlice);
        testTransformedTableLookahead(true, VirtualTableExamples::dataConcatenateAndSlice, VirtualTableExamples::vtConcatenateAndSlice);
    }

    @Test
    public void testConcatenateAndSliceSingleTable() {
        final ColumnarSchema expectedSchema = ColumnarSchema.of(INT);
        final Object[][] expectedValues = new Object[][]{ //
                new Object[]{12}, //
                new Object[]{13}, //
        };
        testTransformedTable(expectedSchema, expectedValues, expectedValues.length, VirtualTableExamples::dataConcatenateAndSlice, VirtualTableExamples::vtConcatenateAndSliceSingleTable);
        testTransformedTableLookahead(true, VirtualTableExamples::dataConcatenateAndSlice, VirtualTableExamples::vtConcatenateAndSliceSingleTable);
    }

    @Test
    public void testConcatenateAndSliceFullSingleTable() {
        final ColumnarSchema expectedSchema = ColumnarSchema.of(INT);
        final Object[][] expectedValues = new Object[][]{ //
                new Object[]{11}, //
                new Object[]{12}, //
                new Object[]{13}, //
                new Object[]{14}, //
        };
        testTransformedTable(expectedSchema, expectedValues, expectedValues.length, VirtualTableExamples::dataConcatenateAndSlice, VirtualTableExamples::vtConcatenateAndSliceFullSingleTable);
        testTransformedTableLookahead(true, VirtualTableExamples::dataConcatenateAndSlice, VirtualTableExamples::vtConcatenateAndSliceFullSingleTable);
    }

    @Test
    public void testConcatenateAndSliceFullTable() {
        final ColumnarSchema expectedSchema = ColumnarSchema.of(INT);
        final Object[][] expectedValues = new Object[][]{ //
                new Object[]{11}, //
                new Object[]{12}, //
                new Object[]{13}, //
                new Object[]{14}, //
                new Object[]{16}, //
        };
        testTransformedTable(expectedSchema, expectedValues, expectedValues.length, VirtualTableExamples::dataConcatenateAndSlice, VirtualTableExamples::vtConcatenateAndSliceFullTable);
        testTransformedTableLookahead(true, VirtualTableExamples::dataConcatenateAndSlice, VirtualTableExamples::vtConcatenateAndSliceFullTable);
    }



    public static VirtualTable vtAppendMissing(final UUID[] sourceIdentifiers, final RowAccessible[] sources) {
        final VirtualTable transformedTable2 = new VirtualTable(sourceIdentifiers[1], new SourceTableProperties(sources[1]));
        return new VirtualTable(sourceIdentifiers[0], new SourceTableProperties(sources[0]))
                .filterColumns(0)
                .appendMissingValueColumns(List.of(DOUBLE.spec()), List.of(DOUBLE.traits()))
                .concatenate(List.of(transformedTable2));
    }

    public static VirtualTable vtAppendMissing() {
        return vtAppendMissing(new UUID[]{randomUUID(), randomUUID()}, dataAppendMissing());
    }

    public static RowAccessible[] dataAppendMissing() {
        final ColumnarSchema schema1 = ColumnarSchema.of(INT, STRING);
        final Object[][] values1 = new Object[][]{ //
                new Object[]{1, "First"}, //
                new Object[]{2, "Second"}, //
                new Object[]{3, "Third"}, //
                new Object[]{4, "Fourth"}, //
                new Object[]{5, "Fifth"} //
        };
        final ColumnarSchema schema2 = ColumnarSchema.of(INT, DOUBLE);
        final Object[][] values2 = new Object[][]{ //
                new Object[]{11, 1.1}, //
                new Object[]{12, 1.2} //
        };
        return new RowAccessible[] {
                RowAccessiblesTestUtils.createRowAccessibleFromRowWiseValues(schema1, values1),
                RowAccessiblesTestUtils.createRowAccessibleFromRowWiseValues(schema2, values2),
        };
    }

    @Test
    public void testAppendMissing() {
        final ColumnarSchema expectedSchema = ColumnarSchema.of(INT, DOUBLE);
        final Object[][] expectedValues = new Object[][]{ //
                new Object[]{1, null}, //
                new Object[]{2, null}, //
                new Object[]{3, null}, //
                new Object[]{4, null}, //
                new Object[]{5, null}, //
                new Object[]{11, 1.1}, //
                new Object[]{12, 1.2} //
        };
        testTransformedTable(expectedSchema, expectedValues, expectedValues.length, VirtualTableExamples::dataAppendMissing, VirtualTableExamples::vtAppendMissing);
        testTransformedTableLookahead(true, VirtualTableExamples::dataAppendMissing, VirtualTableExamples::vtAppendMissing);
    }



    public static VirtualTable vtSimpleMap(final UUID[] sourceIdentifiers, final RowAccessible[] sources) {
        final MapperFactory add = MapperFactory.doublesToDouble((a, b) -> a + b);
        final VirtualTable table = new VirtualTable(sourceIdentifiers[0], new SourceTableProperties(sources[0]));
        final VirtualTable mappedCols = table.map(new int[]{2, 3}, add);
        return table
                .filterColumns(0,1)
                .append(List.of(mappedCols));
    }

    public static VirtualTable vtSimpleMap() {
        return vtSimpleMap(new UUID[]{randomUUID()}, dataSimpleMap());
    }

    public static RowAccessible[] dataSimpleMap() {
        final ColumnarSchema schema = ColumnarSchema.of(INT, STRING, DOUBLE, DOUBLE);
        final Object[][] values = new Object[][]{ //
                new Object[]{1, "First", 0.1, 1.0}, //
                new Object[]{2, "Second", 0.2, 2.0}, //
                new Object[]{3, "Third", 0.3, 3.0}, //
                new Object[]{4, "Fourth", 0.4, 4.0}, //
                new Object[]{5, "Fifth", 0.5, 5.0}, //
                new Object[]{6, "Sixth", 0.6, 6.0}, //
                new Object[]{7, "Seventh", 0.7, 7.0} //
        };
        return new RowAccessible[]{RowAccessiblesTestUtils.createRowAccessibleFromRowWiseValues(schema, values)};
    }

    @Test
    public void testSimpleMap() {
        final ColumnarSchema expectedSchema = ColumnarSchema.of(INT, STRING, DOUBLE);
        final Object[][] expectedValues = new Object[][]{ //
                new Object[]{1, "First", 1.1}, //
                new Object[]{2, "Second", 2.2}, //
                new Object[]{3, "Third", 3.3}, //
                new Object[]{4, "Fourth", 4.4}, //
                new Object[]{5, "Fifth", 5.5}, //
                new Object[]{6, "Sixth", 6.6}, //
                new Object[]{7, "Seventh", 7.7} //
        };
        testTransformedTable(expectedSchema, expectedValues, expectedValues.length, VirtualTableExamples::dataSimpleMap, VirtualTableExamples::vtSimpleMap);
        testTransformedTableLookahead(true, VirtualTableExamples::dataSimpleMap, VirtualTableExamples::vtSimpleMap);
    }



    public static VirtualTable vtSimpleRowFilter(final UUID[] sourceIdentifiers, final RowAccessible[] sources) {
        final RowFilterFactory isNonNegative = RowFilterFactory.doublePredicate(d -> d >= 0);
        final VirtualTable table = new VirtualTable(sourceIdentifiers[0], new SourceTableProperties(sources[0]));
        final VirtualTable filtered = table.filterRows(new int[]{2}, isNonNegative);
        return filtered;
    }

    public static VirtualTable vtSimpleRowFilter() {
        return vtSimpleRowFilter(new UUID[]{randomUUID()}, dataSimpleRowFilter());
    }

    public static RowAccessible[] dataSimpleRowFilter() {
        final ColumnarSchema schema = ColumnarSchema.of(INT, STRING, DOUBLE, DOUBLE);
        final Object[][] values = new Object[][]{ //
                new Object[]{1, "First", 0.1, 1.0}, //
                new Object[]{2, "Second", -0.2, 2.0}, //
                new Object[]{3, "Third", 0.3, 3.0}, //
                new Object[]{4, "Fourth", -0.4, 4.0}, //
                new Object[]{5, "Fifth", -0.5, 5.0}, //
                new Object[]{6, "Sixth", 0.6, 6.0}, //
                new Object[]{7, "Seventh", -0.7, 7.0} //
        };
        return new RowAccessible[]{RowAccessiblesTestUtils.createRowAccessibleFromRowWiseValues(schema, values)};
    }

    @Test
    public void testSimpleRowFilter() {
        final ColumnarSchema expectedSchema = ColumnarSchema.of(INT, STRING, DOUBLE, DOUBLE);
        final Object[][] expectedValues = new Object[][]{ //
                new Object[]{1, "First", 0.1, 1.0}, //
                new Object[]{3, "Third", 0.3, 3.0}, //
                new Object[]{6, "Sixth", 0.6, 6.0} //
        };
        testTransformedTable(expectedSchema, expectedValues, -1, VirtualTableExamples::dataSimpleRowFilter, VirtualTableExamples::vtSimpleRowFilter);
        testTransformedTableLookahead(false, VirtualTableExamples::dataSimpleRowFilter, VirtualTableExamples::vtSimpleRowFilter);
    }



    public static VirtualTable vtConsecutiveRowFilters(final UUID[] sourceIdentifiers, final RowAccessible[] sources) {
        final RowFilterFactory isNonNegative = RowFilterFactory.doublePredicate(d -> d > 0);
        final RowFilterFactory isEven = RowFilterFactory.intPredicate(i -> i % 2 == 0);
        final VirtualTable table = new VirtualTable(sourceIdentifiers[0], new SourceTableProperties(sources[0]));
        final VirtualTable filtered = table.filterRows(new int[]{2}, isNonNegative).filterRows(new int[]{0}, isEven);
        return filtered;
    }

    public static VirtualTable vtConsecutiveRowFilters() {
        return vtConsecutiveRowFilters(new UUID[]{randomUUID()}, dataConsecutiveRowFilters());
    }

    public static RowAccessible[] dataConsecutiveRowFilters() {
        final ColumnarSchema schema = ColumnarSchema.of(INT, STRING, DOUBLE, DOUBLE);
        final Object[][] values = new Object[][]{ //
                new Object[]{1, "First", 0.1, 1.0}, //
                new Object[]{2, "Second", -0.2, 2.0}, //
                new Object[]{3, "Third", 0.3, 3.0}, //
                new Object[]{4, "Fourth", -0.4, 4.0}, //
                new Object[]{5, "Fifth", -0.5, 5.0}, //
                new Object[]{6, "Sixth", 0.6, 6.0}, //
                new Object[]{7, "Seventh", 0.7, 7.0}, //
                new Object[]{8, "Eighth", 0.8, 8.0}, //
                new Object[]{9, "Ninth", -0.9, 9.0} //
        };
        return new RowAccessible[]{RowAccessiblesTestUtils.createRowAccessibleFromRowWiseValues(schema, values)};
    }

    @Test
    public void testConsecutiveRowFilters() {
        final ColumnarSchema expectedSchema = ColumnarSchema.of(INT, STRING, DOUBLE, DOUBLE);
        final Object[][] expectedValues = new Object[][]{ //
                new Object[]{6, "Sixth", 0.6, 6.0}, //
                new Object[]{8, "Eighth", 0.8, 8.0} //
        };
        testTransformedTable(expectedSchema, expectedValues, -1, VirtualTableExamples::dataConsecutiveRowFilters, VirtualTableExamples::vtConsecutiveRowFilters);
        testTransformedTableLookahead(false, VirtualTableExamples::dataConsecutiveRowFilters, VirtualTableExamples::vtConsecutiveRowFilters);
    }



    public static VirtualTable vtMapsAndFilters(final UUID[] sourceIdentifiers, final RowAccessible[] sources) {
        final RowFilterFactory isEven = RowFilterFactory.intPredicate(i -> i % 2 == 0);
        final RowFilterFactory isGreaterThanFive = RowFilterFactory.doublePredicate(d -> d > 5);

        final VirtualTable table = new VirtualTable(sourceIdentifiers[0], new SourceTableProperties(sources[0]));
        final VirtualTable mappedCols = table.map(new int[]{1, 2}, MapperFactory.doublesToDouble((a, b) -> a + b));
        return table //
                .append(List.of(mappedCols)) //
                .filterRows(new int[]{4}, isGreaterThanFive) //
                .filterRows(new int[]{0}, isEven) //
                .filterColumns(3);
    }

    public static VirtualTable vtMapsAndFilters() {
        return vtMapsAndFilters(new UUID[]{randomUUID()}, dataMapsAndFilters());
    }

    public static RowAccessible[] dataMapsAndFilters() {
        final ColumnarSchema schema = ColumnarSchema.of(INT, DOUBLE, DOUBLE, STRING);
        final Object[][] values = new Object[][]{ //
                new Object[]{1, 0.5, 1.0, "First"}, //
                new Object[]{2, 1.2, 4.0, "Second"}, //
                new Object[]{3, 0.7, 3.0, "Third"}, //
                new Object[]{4, 4.9, 0.2, "Fourth"}, //
                new Object[]{5, 4.7, 3.0, "Fifth"}, //
                new Object[]{6, 1.0, 3.8, "Sixth"}, //
                new Object[]{7, 3.3, 3.2, "Seventh"}, //
        };
        return new RowAccessible[]{RowAccessiblesTestUtils.createRowAccessibleFromRowWiseValues(schema, values)};
    }

    @Test
    public void testMapsAndFilters() {
        final ColumnarSchema expectedSchema = ColumnarSchema.of(STRING);
        final Object[][] expectedValues = new Object[][]{ //
                new Object[]{"Second"}, //
                new Object[]{"Fourth"} //
        };
        testTransformedTable(expectedSchema, expectedValues, -1, VirtualTableExamples::dataMapsAndFilters, VirtualTableExamples::vtMapsAndFilters);
        testTransformedTableLookahead(false, VirtualTableExamples::dataMapsAndFilters, VirtualTableExamples::vtMapsAndFilters);
    }



    public static VirtualTable vtFiltersMapAndConcatenate(final UUID[] sourceIdentifiers, final RowAccessible[] sources) {
        final RowFilterFactory isEven = RowFilterFactory.intPredicate(i -> i % 2 == 0);
        final RowFilterFactory isGreaterThanThree = RowFilterFactory.doublePredicate(d -> d > 3);

        final VirtualTable table = new VirtualTable(sourceIdentifiers[0], new SourceTableProperties(sources[0]));
        final VirtualTable table1 = table //
                .filterRows(new int[]{2}, isGreaterThanThree) //
                .filterRows(new int[]{0}, isEven);
        final VirtualTable mappedCols = table1.map(new int[]{1, 2}, MapperFactory.doublesToDouble(Double::sum));
        final VirtualTable table2 = table1 //
                .append(List.of(mappedCols)) //
                .filterColumns(3, 4);
        final VirtualTable table3 = new VirtualTable(sourceIdentifiers[1], new SourceTableProperties(sources[1])).slice(0,1);
        return table2.concatenate(List.of(table3));
//        return table3.concatenate(List.of(table2));
    }

    public static VirtualTable vtFiltersMapAndConcatenate() {
        return vtFiltersMapAndConcatenate(new UUID[]{randomUUID(), randomUUID()}, dataFiltersMapAndConcatenate());
    }

    public static RowAccessible[] dataFiltersMapAndConcatenate() {
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
        final ColumnarSchema schema2 = ColumnarSchema.of(STRING, DOUBLE);
        final Object[][] values2 = new Object[][]{ //
                new Object[]{"2_First", 1.0}, //
                new Object[]{"2_Second", 2.0}, //
        };
        return new RowAccessible[]{
                RowAccessiblesTestUtils.createRowAccessibleFromRowWiseValues(schema1, values1),
                RowAccessiblesTestUtils.createRowAccessibleFromRowWiseValues(schema2, values2)
        };
    }

    @Test
    public void testFiltersMapAndConcatenate() {
        final ColumnarSchema expectedSchema = ColumnarSchema.of(STRING, DOUBLE);
        final Object[][] expectedValues = new Object[][]{ //
                new Object[]{"Second", 5.2}, //
                new Object[]{"Sixth", 4.8}, //
                new Object[]{"2_First", 1.0} //
        };
        testTransformedTable(expectedSchema, expectedValues, -1, VirtualTableExamples::dataFiltersMapAndConcatenate, VirtualTableExamples::vtFiltersMapAndConcatenate);
        testTransformedTableLookahead(false, VirtualTableExamples::dataFiltersMapAndConcatenate, VirtualTableExamples::vtFiltersMapAndConcatenate);
    }
}
