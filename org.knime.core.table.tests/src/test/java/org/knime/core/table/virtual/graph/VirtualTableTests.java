/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 */
package org.knime.core.table.virtual.graph;

import static java.util.UUID.randomUUID;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.knime.core.table.RowAccessiblesTestUtils.assertCanForwardPredictsForward;
import static org.knime.core.table.RowAccessiblesTestUtils.assertTableEqualsValues;
import static org.knime.core.table.RowAccessiblesTestUtils.assertTableEqualsValuesInRandomRowOrder;
import static org.knime.core.table.RowAccessiblesTestUtils.toLookahead;
import static org.knime.core.table.schema.DataSpecs.DOUBLE;
import static org.knime.core.table.schema.DataSpecs.INT;
import static org.knime.core.table.schema.DataSpecs.STRING;
import static org.knime.core.table.virtual.spec.SourceTableProperties.CursorType.BASIC;
import static org.knime.core.table.virtual.spec.SourceTableProperties.CursorType.LOOKAHEAD;
import static org.knime.core.table.virtual.spec.SourceTableProperties.CursorType.RANDOMACCESS;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import org.junit.Test;
import org.knime.core.table.RowAccessiblesTestUtils;
import org.knime.core.table.access.DoubleAccess;
import org.knime.core.table.access.IntAccess;
import org.knime.core.table.access.StringAccess;
import org.knime.core.table.cursor.Cursor;
import org.knime.core.table.row.LookaheadRowAccessible;
import org.knime.core.table.row.RandomRowAccessible;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.VirtualTable;
import org.knime.core.table.virtual.graph.exec.CapExecutor;
import org.knime.core.table.virtual.graph.rag.TableTransformGraph;
import org.knime.core.table.virtual.graph.rag.TableTransformUtil;
import org.knime.core.table.virtual.spec.MapTransformSpec.MapperFactory;
import org.knime.core.table.virtual.spec.MapTransformUtils;
import org.knime.core.table.virtual.spec.MapTransformUtils.MapperWithRowIndexFactory;
import org.knime.core.table.virtual.spec.ObserverTransformUtils.ObserverWithRowIndexFactory;
import org.knime.core.table.virtual.spec.RowFilterTransformSpec.RowFilterFactory;
import org.knime.core.table.virtual.spec.SourceTableProperties;
import org.knime.core.table.virtual.spec.SourceTableProperties.CursorType;

public class VirtualTableTests {

    private static void testTransformedTable(
            final ColumnarSchema expectedSchema,
            final Object[][] expectedValues,
            final int expectedNumRows,
            final Supplier<RowAccessible[]> sourcesSupplier,
            final BiFunction<UUID[], RowAccessible[], VirtualTable> virtualTableSupplier) {

        testTransformedTable(expectedSchema, expectedValues, expectedNumRows, sourcesSupplier, virtualTableSupplier, false);
        testTransformedTable(expectedSchema, expectedValues, expectedNumRows, sourcesSupplier, virtualTableSupplier, true);
    }

    private static void testTransformedTable(
            final ColumnarSchema expectedSchema,
            final Object[][] expectedValues,
            final int expectedNumRows,
            final Supplier<RowAccessible[]> sourcesSupplier,
            final BiFunction<UUID[], RowAccessible[], VirtualTable> virtualTableSupplier,
            final boolean useRandomAccess) {

        final RowAccessible[] sources = sourcesSupplier.get();
        final UUID[] sourceIds = new UUID[sources.length];
        Arrays.setAll(sourceIds, i -> randomUUID());
        final VirtualTable table = virtualTableSupplier.apply(sourceIds, sources);

        final RowAccessible rowAccessible = createRowAccessible(table, sourceIds, sources, useRandomAccess);

        if ( expectedNumRows < 0 ) {
            assertTrue(rowAccessible.size() < 0);
        } else {
            assertEquals(expectedNumRows, rowAccessible.size());
        }
        assertEquals(expectedSchema, table.getSchema());
        assertTableEqualsValues(expectedValues, rowAccessible, false);
        assertTableEqualsValues(expectedValues, rowAccessible, true);
    }

    private static RowAccessible createRowAccessible(
            final VirtualTable table,
            final UUID[] sourceIds,
            final RowAccessible[] sources,
            final boolean useRandomAccess) {

        final TableTransformGraph graph = new TableTransformGraph(table.getProducingTransform());
        TableTransformUtil.optimize(graph);
        final CursorType cursorType = switch (graph.supportedCursorType()) {
            case BASIC -> BASIC;
            case LOOKAHEAD -> LOOKAHEAD;
            case RANDOMACCESS -> useRandomAccess ? RANDOMACCESS : LOOKAHEAD;
        };
        final Map<UUID, RowAccessible> sourceMap = new HashMap<>();
        for (int i = 0; i < sourceIds.length; ++i) {
            sourceMap.put(sourceIds[i], sources[i]);
        }
        return CapExecutor.createRowAccessible(graph, cursorType, sourceMap);
    }

    @FunctionalInterface
    private interface TriFunction<A,B,C,T> {
        T apply(A a, B b, C c);
    }

    private static void testTransformedTableObservations(
            final Object[][] expectedObservations,
            final Supplier<RowAccessible[]> sourcesSupplier,
            final TriFunction<UUID[], RowAccessible[], List<Object[]>, VirtualTable> virtualTableSupplier) {

        testTransformedTableObservations(expectedObservations, sourcesSupplier, virtualTableSupplier, false);
        testTransformedTableObservations(expectedObservations, sourcesSupplier, virtualTableSupplier, true);
    }

    private static void testTransformedTableObservations(
            final Object[][] expectedObservations,
            final Supplier<RowAccessible[]> sourcesSupplier,
            final TriFunction<UUID[], RowAccessible[], List<Object[]>, VirtualTable> virtualTableSupplier,
            final boolean useRandomAccess) {

        final RowAccessible[] sources = sourcesSupplier.get();
        final UUID[] sourceIds = new UUID[sources.length];
        Arrays.setAll(sourceIds, i -> randomUUID());
        final List<Object[]> observations = new ArrayList<>();
        final VirtualTable table = virtualTableSupplier.apply(sourceIds, sources, observations);

        try (final RowAccessible rowAccessible = createRowAccessible(table, sourceIds, sources, useRandomAccess)) {
            try (final Cursor<ReadAccessRow> cursor = rowAccessible.createCursor()) {
                while (cursor.forward()) {
                }
                assertEquals(expectedObservations.length, observations.size());
                for (int i = 0; i < observations.size(); i++) {
                    assertArrayEquals(expectedObservations[i], observations.get(i));
                }
            }
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    private static void testTransformedTableLookahead(
            final boolean expectedLookahead,
            final Supplier<RowAccessible[]> sourcesSupplier,
            final BiFunction<UUID[], RowAccessible[], VirtualTable> virtualTableSupplier) {

        final RowAccessible[] sources = toLookahead(sourcesSupplier.get());
        final UUID[] sourceIds = new UUID[sources.length];
        Arrays.setAll(sourceIds, i -> randomUUID());
        final VirtualTable table = virtualTableSupplier.apply(sourceIds, sources);

        final RowAccessible rowAccessible = createRowAccessible(table, sourceIds, sources, true);
        final boolean lookahead = rowAccessible instanceof LookaheadRowAccessible;

        assertEquals(expectedLookahead, lookahead);
        if (lookahead) {
            assertCanForwardPredictsForward(rowAccessible);
        }
    }

    private static void testTransformedTableRandomAccess(
            final boolean expectedRandomAccessible,
            final ColumnarSchema expectedSchema,
            final Object[][] expectedValues,
            final int expectedNumRows,
            final Supplier<RowAccessible[]> sourcesSupplier,
            final BiFunction<UUID[], RowAccessible[], VirtualTable> virtualTableSupplier) {

        final RowAccessible[] sources = sourcesSupplier.get();
        final UUID[] sourceIds = new UUID[sources.length];
        Arrays.setAll(sourceIds, i -> randomUUID());
        final VirtualTable table = virtualTableSupplier.apply(sourceIds, sources);

        final RowAccessible rowAccessible = createRowAccessible(table, sourceIds, sources, true);
        final boolean randomAccessible = rowAccessible instanceof RandomRowAccessible;

        if ( expectedNumRows < 0 ) {
            assertTrue(rowAccessible.size() < 0);
        } else {
            assertEquals(expectedNumRows, rowAccessible.size());
        }
        assertEquals(expectedRandomAccessible, randomAccessible);
        if (randomAccessible) {
            assertEquals(expectedSchema, table.getSchema());
            assertTableEqualsValuesInRandomRowOrder(expectedValues, rowAccessible, false);
            assertTableEqualsValuesInRandomRowOrder(expectedValues, rowAccessible, true);
        }
    }



    public static VirtualTable vtMinimal(final UUID[] sourceIdentifiers, final RowAccessible[] sources) {
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
        testTransformedTable(expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataMinimal, VirtualTableTests::vtMinimal);
        testTransformedTableLookahead(true, VirtualTableTests::dataMinimal, VirtualTableTests::vtMinimal);
        testTransformedTableRandomAccess(true, expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataMinimal, VirtualTableTests::vtMinimal);
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
        testTransformedTable(expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataLinear, VirtualTableTests::vtLinear);
        testTransformedTableLookahead(true, VirtualTableTests::dataLinear, VirtualTableTests::vtLinear);
        testTransformedTableRandomAccess(true, expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataLinear, VirtualTableTests::vtLinear);
    }



    public static VirtualTable vtForkJoin(final UUID[] sourceIdentifiers, final RowAccessible[] sources) {
        final VirtualTable transformedTable = new VirtualTable(sourceIdentifiers[0], new SourceTableProperties(sources[0])).slice(1, 6);
        final VirtualTable forkedTable1 = transformedTable.filterColumns(1).appendMissingValueColumns(DOUBLE);
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
        testTransformedTable(expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataForkJoin, VirtualTableTests::vtForkJoin);
        testTransformedTableLookahead(true, VirtualTableTests::dataForkJoin, VirtualTableTests::vtForkJoin);
        testTransformedTableRandomAccess(true, expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataForkJoin, VirtualTableTests::vtForkJoin);
    }



    public static VirtualTable vtForkJoinLookALike(final UUID[] sourceIdentifiers, final RowAccessible[] sources) {
        final VirtualTable transformedTable = new VirtualTable(sourceIdentifiers[0], new SourceTableProperties(sources[0])).slice(1, 6);
        final VirtualTable forkedTable1 = transformedTable.filterColumns(1).appendMissingValueColumns(DOUBLE);
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
        testTransformedTable(expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataForkJoinLookALike, VirtualTableTests::vtForkJoinLookALike);
        testTransformedTableLookahead(true, VirtualTableTests::dataForkJoinLookALike, VirtualTableTests::vtForkJoinLookALike);
        testTransformedTableRandomAccess(true, expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataForkJoinLookALike, VirtualTableTests::vtForkJoinLookALike);
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
        testTransformedTable(expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataAppend, VirtualTableTests::vtAppend);
        testTransformedTableLookahead(true, VirtualTableTests::dataAppend, VirtualTableTests::vtAppend);
        testTransformedTableRandomAccess(true, expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataAppend, VirtualTableTests::vtAppend);
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
        testTransformedTable(expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataAppendAndFilterColumns, VirtualTableTests::vtAppendAndFilterColumns);
        testTransformedTableLookahead(true, VirtualTableTests::dataAppendAndFilterColumns, VirtualTableTests::vtAppendAndFilterColumns);
        testTransformedTableRandomAccess(true, expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataAppendAndFilterColumns, VirtualTableTests::vtAppendAndFilterColumns);
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
        testTransformedTable(expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataAppendAndSlice, VirtualTableTests::vtAppendAndSlice);
        testTransformedTableLookahead(true, VirtualTableTests::dataAppendAndSlice, VirtualTableTests::vtAppendAndSlice);
        testTransformedTableRandomAccess(true, expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataAppendAndSlice, VirtualTableTests::vtAppendAndSlice);
    }



    public static VirtualTable vtAppendAndAppendMissing(final UUID[] sourceIdentifiers, final RowAccessible[] sources) {
        final VirtualTable transformedTable2 = new VirtualTable(sourceIdentifiers[1], new SourceTableProperties(sources[1]))
                .permute(1, 0)
                .appendMissingValueColumns(DOUBLE);
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
        testTransformedTable(expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataAppendAndAppendMissing, VirtualTableTests::vtAppendAndAppendMissing);
        testTransformedTableLookahead(true, VirtualTableTests::dataAppendAndAppendMissing, VirtualTableTests::vtAppendAndAppendMissing);
        testTransformedTableRandomAccess(true, expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataAppendAndAppendMissing, VirtualTableTests::vtAppendAndAppendMissing);
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
        testTransformedTable(expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataConcatenate, VirtualTableTests::vtConcatenate);
        testTransformedTableLookahead(true, VirtualTableTests::dataConcatenate, VirtualTableTests::vtConcatenate);
        testTransformedTableRandomAccess(true, expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataConcatenate, VirtualTableTests::vtConcatenate);
    }



    public static VirtualTable vtConcatenateSelf(final UUID[] sourceIdentifiers, final RowAccessible[] sources) {
        final ColumnarSchema schema = ColumnarSchema.of(DOUBLE, INT, STRING);
        final VirtualTable table1 = new VirtualTable(sourceIdentifiers[0], new SourceTableProperties(sources[0]));
        return table1.concatenate(table1);
    }

    public static VirtualTable vtConcatenateSelf() {
        return vtConcatenateSelf(new UUID[]{randomUUID()}, dataMinimal());
    }

    public static RowAccessible[] dataConcatenateSelf() {
        final ColumnarSchema schema = ColumnarSchema.of(STRING);
        final Object[][] values = new Object[][]{ //
                new Object[]{"First"}, //
                new Object[]{"Second"}, //
        };
        return new RowAccessible[]{RowAccessiblesTestUtils.createRowAccessibleFromRowWiseValues(schema, values)};
    }

    @Test
    public void testConcatenateSelf() throws IOException {
        final ColumnarSchema expectedSchema = ColumnarSchema.of(STRING);
        final Object[][] expectedValues = new Object[][]{ //
                new Object[]{"First"}, //
                new Object[]{"Second"}, //
                new Object[]{"First"}, //
                new Object[]{"Second"}, //
        };
        testTransformedTable(expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataConcatenateSelf, VirtualTableTests::vtConcatenateSelf);
        testTransformedTableLookahead(true, VirtualTableTests::dataConcatenateSelf, VirtualTableTests::vtConcatenateSelf);
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
        testTransformedTable(expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataConcatenateAndSlice, VirtualTableTests::vtConcatenateAndSlice);
        testTransformedTableLookahead(true, VirtualTableTests::dataConcatenateAndSlice, VirtualTableTests::vtConcatenateAndSlice);
        testTransformedTableRandomAccess(true, expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataConcatenateAndSlice, VirtualTableTests::vtConcatenateAndSlice);
    }

    @Test
    public void testConcatenateAndSliceSingleTable() {
        final ColumnarSchema expectedSchema = ColumnarSchema.of(INT);
        final Object[][] expectedValues = new Object[][]{ //
                new Object[]{12}, //
                new Object[]{13}, //
        };
        testTransformedTable(expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataConcatenateAndSlice, VirtualTableTests::vtConcatenateAndSliceSingleTable);
        testTransformedTableLookahead(true, VirtualTableTests::dataConcatenateAndSlice, VirtualTableTests::vtConcatenateAndSliceSingleTable);
        testTransformedTableRandomAccess(true, expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataConcatenateAndSlice, VirtualTableTests::vtConcatenateAndSliceSingleTable);
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
        testTransformedTable(expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataConcatenateAndSlice, VirtualTableTests::vtConcatenateAndSliceFullSingleTable);
        testTransformedTableLookahead(true, VirtualTableTests::dataConcatenateAndSlice, VirtualTableTests::vtConcatenateAndSliceFullSingleTable);
        testTransformedTableRandomAccess(true, expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataConcatenateAndSlice, VirtualTableTests::vtConcatenateAndSliceFullSingleTable);
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
        testTransformedTable(expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataConcatenateAndSlice, VirtualTableTests::vtConcatenateAndSliceFullTable);
        testTransformedTableLookahead(true, VirtualTableTests::dataConcatenateAndSlice, VirtualTableTests::vtConcatenateAndSliceFullTable);
        testTransformedTableRandomAccess(true, expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataConcatenateAndSlice, VirtualTableTests::vtConcatenateAndSliceFullTable);
    }



    public static VirtualTable vtAppendMissing(final UUID[] sourceIdentifiers, final RowAccessible[] sources) {
        final VirtualTable transformedTable2 = new VirtualTable(sourceIdentifiers[1], new SourceTableProperties(sources[1]));
        return new VirtualTable(sourceIdentifiers[0], new SourceTableProperties(sources[0]))
                .filterColumns(0)
                .appendMissingValueColumns(DOUBLE)
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
        testTransformedTable(expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataAppendMissing, VirtualTableTests::vtAppendMissing);
        testTransformedTableLookahead(true, VirtualTableTests::dataAppendMissing, VirtualTableTests::vtAppendMissing);
        testTransformedTableRandomAccess(true, expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataAppendMissing, VirtualTableTests::vtAppendMissing);
    }



    public static VirtualTable vtSimpleMap(final UUID[] sourceIdentifiers, final RowAccessible[] sources) {
        final MapperFactory add = MapTransformUtils.doublesToDouble((a, b) -> a + b);
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
        testTransformedTable(expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataSimpleMap, VirtualTableTests::vtSimpleMap);
        testTransformedTableLookahead(true, VirtualTableTests::dataSimpleMap, VirtualTableTests::vtSimpleMap);
        testTransformedTableRandomAccess(true, expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataSimpleMap, VirtualTableTests::vtSimpleMap);
    }




    public static VirtualTable vtSimpleAppendMap(final UUID[] sourceIdentifiers, final RowAccessible[] sources) {
        final MapperFactory add = MapTransformUtils.doublesToDouble((a, b) -> a + b);
        final VirtualTable table = new VirtualTable(sourceIdentifiers[0], new SourceTableProperties(sources[0]));
        return table.appendMap(new int[]{2, 3}, add).selectColumns(0, 1, 4);
    }

    public static VirtualTable vtSimpleAppendMap() {
        return vtSimpleAppendMap(new UUID[]{randomUUID()}, dataSimpleAppendMap());
    }

    public static RowAccessible[] dataSimpleAppendMap() {
        return dataSimpleMap();
    }

    @Test
    public void testSimpleAppendMap() {
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
        testTransformedTable(expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataSimpleAppendMap, VirtualTableTests::vtSimpleAppendMap);
        testTransformedTableLookahead(true, VirtualTableTests::dataSimpleAppendMap, VirtualTableTests::vtSimpleAppendMap);
        testTransformedTableRandomAccess(true, expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataSimpleAppendMap, VirtualTableTests::vtSimpleAppendMap);
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
        testTransformedTable(expectedSchema, expectedValues, -1, VirtualTableTests::dataSimpleRowFilter, VirtualTableTests::vtSimpleRowFilter);
        testTransformedTableLookahead(false, VirtualTableTests::dataSimpleRowFilter, VirtualTableTests::vtSimpleRowFilter);
        testTransformedTableRandomAccess(false, expectedSchema, expectedValues, -1, VirtualTableTests::dataSimpleRowFilter, VirtualTableTests::vtSimpleRowFilter);
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
        testTransformedTable(expectedSchema, expectedValues, -1, VirtualTableTests::dataConsecutiveRowFilters, VirtualTableTests::vtConsecutiveRowFilters);
        testTransformedTableLookahead(false, VirtualTableTests::dataConsecutiveRowFilters, VirtualTableTests::vtConsecutiveRowFilters);
        testTransformedTableRandomAccess(false, expectedSchema, expectedValues, -1, VirtualTableTests::dataConsecutiveRowFilters, VirtualTableTests::vtConsecutiveRowFilters);
    }



    public static VirtualTable vtMapsAndFilters(final UUID[] sourceIdentifiers, final RowAccessible[] sources) {
        final RowFilterFactory isEven = RowFilterFactory.intPredicate(i -> i % 2 == 0);
        final RowFilterFactory isGreaterThanFive = RowFilterFactory.doublePredicate(d -> d > 5);

        final VirtualTable table = new VirtualTable(sourceIdentifiers[0], new SourceTableProperties(sources[0]));
        final VirtualTable mappedCols = table.map(new int[]{1, 2}, MapTransformUtils.doublesToDouble((a, b) -> a + b));
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
        testTransformedTable(expectedSchema, expectedValues, -1, VirtualTableTests::dataMapsAndFilters, VirtualTableTests::vtMapsAndFilters);
        testTransformedTableLookahead(false, VirtualTableTests::dataMapsAndFilters, VirtualTableTests::vtMapsAndFilters);
        testTransformedTableRandomAccess(false, expectedSchema, expectedValues, -1, VirtualTableTests::dataMapsAndFilters, VirtualTableTests::vtMapsAndFilters);
    }



    public static VirtualTable vtFiltersMapAndConcatenate(final UUID[] sourceIdentifiers, final RowAccessible[] sources) {
        final RowFilterFactory isEven = RowFilterFactory.intPredicate(i -> i % 2 == 0);
        final RowFilterFactory isGreaterThanThree = RowFilterFactory.doublePredicate(d -> d > 3);

        final VirtualTable table = new VirtualTable(sourceIdentifiers[0], new SourceTableProperties(sources[0]));
        final VirtualTable table1 = table //
                .filterRows(new int[]{2}, isGreaterThanThree) //
                .filterRows(new int[]{0}, isEven);
        final VirtualTable mappedCols = table1.map(new int[]{1, 2}, MapTransformUtils.doublesToDouble(Double::sum));
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
        testTransformedTable(expectedSchema, expectedValues, -1, VirtualTableTests::dataFiltersMapAndConcatenate, VirtualTableTests::vtFiltersMapAndConcatenate);
        testTransformedTableLookahead(false, VirtualTableTests::dataFiltersMapAndConcatenate, VirtualTableTests::vtFiltersMapAndConcatenate);
        testTransformedTableRandomAccess(false, expectedSchema, expectedValues, -1, VirtualTableTests::dataFiltersMapAndConcatenate, VirtualTableTests::vtFiltersMapAndConcatenate);
    }



    public static VirtualTable vtRowIndexMap(final UUID[] sourceIdentifiers, final RowAccessible[] sources) {
        final MapperWithRowIndexFactory addRowIndex = MapTransformUtils.MapperWithRowIndexFactory.of( //
                ColumnarSchema.of(DOUBLE), //
                (inputs, outputs) -> {
                    MapTransformUtils.verify(inputs, 1, outputs, 1);
                    final DoubleAccess.DoubleReadAccess i = (DoubleAccess.DoubleReadAccess)inputs[0];
                    final DoubleAccess.DoubleWriteAccess o = (DoubleAccess.DoubleWriteAccess)outputs[0];
                    return rowIndex -> o.setDoubleValue(i.getDoubleValue() + rowIndex);
                });
        final VirtualTable table = new VirtualTable(sourceIdentifiers[0], new SourceTableProperties(sources[0]));
        final VirtualTable mappedCols = table.map(new int[]{0}, addRowIndex);
        return mappedCols.append(table.filterColumns(2));
    }

    public static VirtualTable vtRowIndexMap() {
        return vtRowIndexMap(new UUID[]{randomUUID()}, dataMinimal());
    }

    @Test
    public void testRowIndexMap() {
        final ColumnarSchema expectedSchema = ColumnarSchema.of(DOUBLE, STRING);
        final Object[][] expectedValues = new Object[][]{ //
                new Object[]{0.1, "First"}, //
                new Object[]{1.2, "Second"}, //
                new Object[]{2.3, "Third"}, //
                new Object[]{3.4, "Fourth"}, //
                new Object[]{4.5, "Fifth"} //
        };
        testTransformedTable(expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataMinimal, VirtualTableTests::vtRowIndexMap);
        testTransformedTableLookahead(true, VirtualTableTests::dataMinimal, VirtualTableTests::vtRowIndexMap);
        testTransformedTableRandomAccess(true, expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataMinimal, VirtualTableTests::vtRowIndexMap);
    }



    public static VirtualTable vtRowIndexMapAndSlice(final UUID[] sourceIdentifiers, final RowAccessible[] sources) {
        final MapperWithRowIndexFactory addRowIndex = MapTransformUtils.MapperWithRowIndexFactory.of( //
                ColumnarSchema.of(DOUBLE), //
                (inputs, outputs) -> {
                    MapTransformUtils.verify(inputs, 1, outputs, 1);
                    final DoubleAccess.DoubleReadAccess i = (DoubleAccess.DoubleReadAccess)inputs[0];
                    final DoubleAccess.DoubleWriteAccess o = (DoubleAccess.DoubleWriteAccess)outputs[0];
                    return rowIndex -> o.setDoubleValue(i.getDoubleValue() + rowIndex);
                });
        final VirtualTable table = new VirtualTable(sourceIdentifiers[0], new SourceTableProperties(sources[0]));
        final VirtualTable mappedCols = table.map(new int[]{0}, addRowIndex);
        return mappedCols.append(table.filterColumns(2)).slice(2, 4);
    }

    public static VirtualTable vtRowIndexMapAndSlice() {
        return vtRowIndexMapAndSlice(new UUID[]{randomUUID()}, dataMinimal());
    }

    @Test
    public void testRowIndexMapAndSlice() {
        final ColumnarSchema expectedSchema = ColumnarSchema.of(DOUBLE, STRING);
        final Object[][] expectedValues = new Object[][]{ //
                new Object[]{2.3, "Third"}, //
                new Object[]{3.4, "Fourth"}, //
        };
        testTransformedTable(expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataMinimal, VirtualTableTests::vtRowIndexMapAndSlice);
        testTransformedTableLookahead(true, VirtualTableTests::dataMinimal, VirtualTableTests::vtRowIndexMapAndSlice);
        testTransformedTableRandomAccess(true, expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataMinimal, VirtualTableTests::vtRowIndexMapAndSlice);
    }



    public static VirtualTable vtRowIndexMapsParallel(final UUID[] sourceIdentifiers, final RowAccessible[] sources) {
        final MapTransformUtils.MapperWithRowIndexFactory addRowIndex = MapTransformUtils.MapperWithRowIndexFactory.of( //
                ColumnarSchema.of(DOUBLE), //
                (inputs, outputs) -> {
                    MapTransformUtils.verify(inputs, 1, outputs, 1);
                    final DoubleAccess.DoubleReadAccess i = (DoubleAccess.DoubleReadAccess)inputs[0];
                    final DoubleAccess.DoubleWriteAccess o = (DoubleAccess.DoubleWriteAccess)outputs[0];
                    return rowIndex -> o.setDoubleValue(i.getDoubleValue() + rowIndex);
                });
        final MapperWithRowIndexFactory appendRowIndex = MapTransformUtils.MapperWithRowIndexFactory.of( //
                ColumnarSchema.of(STRING), //
                (inputs, outputs) -> {
                    MapTransformUtils.verify(inputs, 1, outputs, 1);
                    final StringAccess.StringReadAccess i = (StringAccess.StringReadAccess)inputs[0];
                    final StringAccess.StringWriteAccess o = (StringAccess.StringWriteAccess)outputs[0];
                    return rowIndex -> o.setStringValue(i.getStringValue() + "-" + rowIndex);
                });
        final VirtualTable table = new VirtualTable(sourceIdentifiers[0], new SourceTableProperties(sources[0]));
        final VirtualTable mappedCols1 = table.map(new int[]{0}, addRowIndex);
        final VirtualTable mappedCols2 = table.map(new int[]{2}, appendRowIndex);
        return mappedCols1.append(table.filterColumns(1)).append(mappedCols2);
    }

    public static VirtualTable vtRowIndexMapsParallel() {
        return vtRowIndexMapsParallel(new UUID[]{randomUUID()}, dataMinimal());
    }

    @Test
    public void testRowIndexMapsParallel() {
        final ColumnarSchema expectedSchema = ColumnarSchema.of(DOUBLE, INT, STRING);
        final Object[][] expectedValues = new Object[][]{ //
                new Object[]{0.1, 1, "First-0"}, //
                new Object[]{1.2, 2, "Second-1"}, //
                new Object[]{2.3, 3, "Third-2"}, //
                new Object[]{3.4, 4, "Fourth-3"}, //
                new Object[]{4.5, 5, "Fifth-4"} //
        };
        testTransformedTable(expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataMinimal, VirtualTableTests::vtRowIndexMapsParallel);
        testTransformedTableLookahead(true, VirtualTableTests::dataMinimal, VirtualTableTests::vtRowIndexMapsParallel);
        testTransformedTableRandomAccess(true, expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataMinimal, VirtualTableTests::vtRowIndexMapsParallel);
    }



    public static VirtualTable vtRowIndexMapsParallelAndSlice(final UUID[] sourceIdentifiers, final RowAccessible[] sources) {
        final MapperWithRowIndexFactory addRowIndex = MapTransformUtils.MapperWithRowIndexFactory.of( //
                ColumnarSchema.of(DOUBLE), //
                (inputs, outputs) -> {
                    MapTransformUtils.verify(inputs, 1, outputs, 1);
                    final DoubleAccess.DoubleReadAccess i = (DoubleAccess.DoubleReadAccess)inputs[0];
                    final DoubleAccess.DoubleWriteAccess o = (DoubleAccess.DoubleWriteAccess)outputs[0];
                    return rowIndex -> o.setDoubleValue(i.getDoubleValue() + rowIndex);
                });
        final MapperWithRowIndexFactory appendRowIndex = MapTransformUtils.MapperWithRowIndexFactory.of( //
                ColumnarSchema.of(STRING), //
                (inputs, outputs) -> {
                    MapTransformUtils.verify(inputs, 1, outputs, 1);
                    final StringAccess.StringReadAccess i = (StringAccess.StringReadAccess)inputs[0];
                    final StringAccess.StringWriteAccess o = (StringAccess.StringWriteAccess)outputs[0];
                    return rowIndex -> o.setStringValue(i.getStringValue() + "-" + rowIndex);
                });
        final VirtualTable table = new VirtualTable(sourceIdentifiers[0], new SourceTableProperties(sources[0]));
        final VirtualTable mappedCols1 = table.map(new int[]{0}, addRowIndex);
        final VirtualTable mappedCols2 = table.map(new int[]{2}, appendRowIndex);
        return mappedCols1.append(table.filterColumns(1)).append(mappedCols2).slice(2, 4);
    }

    public static VirtualTable vtRowIndexMapsParallelAndSlice() {
        return vtRowIndexMapsParallelAndSlice(new UUID[]{randomUUID()}, dataMinimal());
    }

    @Test
    public void testRowIndexMapsParallelAndSlice() {
        final ColumnarSchema expectedSchema = ColumnarSchema.of(DOUBLE, INT, STRING);
        final Object[][] expectedValues = new Object[][]{ //
                new Object[]{2.3, 3, "Third-2"}, //
                new Object[]{3.4, 4, "Fourth-3"}, //
        };
        testTransformedTable(expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataMinimal, VirtualTableTests::vtRowIndexMapsParallelAndSlice);
        testTransformedTableLookahead(true, VirtualTableTests::dataMinimal, VirtualTableTests::vtRowIndexMapsParallelAndSlice);
        testTransformedTableRandomAccess(true, expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataMinimal, VirtualTableTests::vtRowIndexMapsParallelAndSlice);
    }



    public static VirtualTable vtRowIndexMapsSequential(final UUID[] sourceIdentifiers, final RowAccessible[] sources) {
        final MapperWithRowIndexFactory addRowIndex = MapTransformUtils.MapperWithRowIndexFactory.of( //
                ColumnarSchema.of(DOUBLE), //
                (inputs, outputs) -> {
                    MapTransformUtils.verify(inputs, 1, outputs, 1);
                    final DoubleAccess.DoubleReadAccess i = (DoubleAccess.DoubleReadAccess)inputs[0];
                    final DoubleAccess.DoubleWriteAccess o = (DoubleAccess.DoubleWriteAccess)outputs[0];
                    return rowIndex -> o.setDoubleValue(i.getDoubleValue() + rowIndex);
                });
        final MapperWithRowIndexFactory appendRowIndex = MapTransformUtils.MapperWithRowIndexFactory.of( //
                ColumnarSchema.of(STRING), //
                (inputs, outputs) -> {
                    MapTransformUtils.verify(inputs, 1, outputs, 1);
                    final StringAccess.StringReadAccess i = (StringAccess.StringReadAccess)inputs[0];
                    final StringAccess.StringWriteAccess o = (StringAccess.StringWriteAccess)outputs[0];
                    return rowIndex -> o.setStringValue(i.getStringValue() + "-" + rowIndex);
                });
        final VirtualTable table = new VirtualTable(sourceIdentifiers[0], new SourceTableProperties(sources[0]));
        final VirtualTable mappedCols1 = table.map(new int[]{0}, addRowIndex);
        final VirtualTable table2 = table.append(mappedCols1);
        final VirtualTable mappedCols2 = table2.map(new int[]{2}, appendRowIndex);
        return table2.append(mappedCols2).selectColumns(3, 1, 4);
    }

    public static VirtualTable vtRowIndexMapsSequential() {
        return vtRowIndexMapsSequential(new UUID[]{randomUUID()}, dataMinimal());
    }

    @Test
    public void testRowIndexMapsSequential() {
        final ColumnarSchema expectedSchema = ColumnarSchema.of(DOUBLE, INT, STRING);
        final Object[][] expectedValues = new Object[][]{ //
                new Object[]{0.1, 1, "First-0"}, //
                new Object[]{1.2, 2, "Second-1"}, //
                new Object[]{2.3, 3, "Third-2"}, //
                new Object[]{3.4, 4, "Fourth-3"}, //
                new Object[]{4.5, 5, "Fifth-4"} //
        };
        testTransformedTable(expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataMinimal, VirtualTableTests::vtRowIndexMapsSequential);
        testTransformedTableLookahead(true, VirtualTableTests::dataMinimal, VirtualTableTests::vtRowIndexMapsSequential);
        testTransformedTableRandomAccess(true, expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataMinimal, VirtualTableTests::vtRowIndexMapsSequential);
    }



    public static VirtualTable vtRowIndexMapsSequentialAndSlice(final UUID[] sourceIdentifiers, final RowAccessible[] sources) {
        final MapperWithRowIndexFactory addRowIndex = MapTransformUtils.MapperWithRowIndexFactory.of( //
                ColumnarSchema.of(DOUBLE), //
                (inputs, outputs) -> {
                    MapTransformUtils.verify(inputs, 1, outputs, 1);
                    final DoubleAccess.DoubleReadAccess i = (DoubleAccess.DoubleReadAccess)inputs[0];
                    final DoubleAccess.DoubleWriteAccess o = (DoubleAccess.DoubleWriteAccess)outputs[0];
                    return rowIndex -> o.setDoubleValue(i.getDoubleValue() + rowIndex);
                });
        final MapperWithRowIndexFactory appendRowIndex = MapTransformUtils.MapperWithRowIndexFactory.of( //
                ColumnarSchema.of(STRING), //
                (inputs, outputs) -> {
                    MapTransformUtils.verify(inputs, 1, outputs, 1);
                    final StringAccess.StringReadAccess i = (StringAccess.StringReadAccess)inputs[0];
                    final StringAccess.StringWriteAccess o = (StringAccess.StringWriteAccess)outputs[0];
                    return rowIndex -> o.setStringValue(i.getStringValue() + "-" + rowIndex);
                });
        final VirtualTable table = new VirtualTable(sourceIdentifiers[0], new SourceTableProperties(sources[0]));
        final VirtualTable mappedCols1 = table.map(new int[]{0}, addRowIndex);
        final VirtualTable table2 = table.append(mappedCols1);
        final VirtualTable mappedCols2 = table2.map(new int[]{2}, appendRowIndex);
        return table2.append(mappedCols2).selectColumns(3, 1, 4).slice(2, 4);
    }

    public static VirtualTable vtRowIndexMapsSequentialAndSlice() {
        return vtRowIndexMapsSequentialAndSlice(new UUID[]{randomUUID()}, dataMinimal());
    }

    @Test
    public void testRowIndexMapsSequentialAndSlice() {
        final ColumnarSchema expectedSchema = ColumnarSchema.of(DOUBLE, INT, STRING);
        final Object[][] expectedValues = new Object[][]{ //
                new Object[]{2.3, 3, "Third-2"}, //
                new Object[]{3.4, 4, "Fourth-3"}, //
        };
        testTransformedTable(expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataMinimal, VirtualTableTests::vtRowIndexMapsSequentialAndSlice);
        testTransformedTableLookahead(true, VirtualTableTests::dataMinimal, VirtualTableTests::vtRowIndexMapsSequentialAndSlice);
        testTransformedTableRandomAccess(true, expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataMinimal, VirtualTableTests::vtRowIndexMapsSequentialAndSlice);
    }


    public static VirtualTable vtObserve(final UUID[] sourceIdentifiers, final RowAccessible[] sources) {
        return vtObserve(sourceIdentifiers, sources, new ArrayList<>());
    }

    public static VirtualTable vtObserve() {
        return vtObserve(new UUID[]{randomUUID(), randomUUID()}, dataObserve());
    }

    public static RowAccessible[] dataObserve() {
        return dataAppend();
    }

    public static VirtualTable vtObserve(final UUID[] sourceIdentifiers, final RowAccessible[] sources, final List<Object[]> observations) {
        final VirtualTable transformedTable2 = new VirtualTable(sourceIdentifiers[1], new SourceTableProperties(sources[1])).selectColumns(1, 0);
        final ObserverWithRowIndexFactory factory = inputs -> {
            final IntAccess.IntReadAccess i0 = (IntAccess.IntReadAccess)inputs[0];
            final DoubleAccess.DoubleReadAccess i1 = (DoubleAccess.DoubleReadAccess)inputs[1];
            return rowIndex -> {
                observations.add(new Object[]{i0.getIntValue(), i1.getDoubleValue(), rowIndex});
            };
        };
        return new VirtualTable(sourceIdentifiers[0], new SourceTableProperties(sources[0])).selectColumns(1, 2)
                .append(transformedTable2).selectColumns(0, 2).slice(1, 4).observe(new int[]{0, 1}, factory);
    }

    @Test
    public void testObserve() {
        final ColumnarSchema expectedSchema = ColumnarSchema.of(INT, DOUBLE);
        final Object[][] expectedValues = new Object[][]{ //
                new Object[]{2, 1.2}, //
                new Object[]{3, 1.3}, //
                new Object[]{4, 1.4} //
        };
        final Object[][] expectedObservations = new Object[][]{ //
                new Object[]{2, 1.2, 0L}, //
                new Object[]{3, 1.3, 1L}, //
                new Object[]{4, 1.4, 2L} //
        };
        testTransformedTableObservations(expectedObservations, VirtualTableTests::dataObserve, VirtualTableTests::vtObserve);
        testTransformedTable(expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataObserve, VirtualTableTests::vtObserve);
        testTransformedTableLookahead(true, VirtualTableTests::dataObserve, VirtualTableTests::vtObserve);
        testTransformedTableRandomAccess(true, expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataObserve, VirtualTableTests::vtObserve);
    }



    public static VirtualTable vtRowIndexToRowKeyMap(final UUID[] sourceIdentifiers, final RowAccessible[] sources) {
        final MapperWithRowIndexFactory createRowKey = MapTransformUtils.MapperWithRowIndexFactory.of( //
                ColumnarSchema.of(STRING), //
                (inputs, outputs) -> {
                    MapTransformUtils.verify(inputs, 0, outputs, 1);
                    final StringAccess.StringWriteAccess o = (StringAccess.StringWriteAccess)outputs[0];
                    return rowIndex -> o.setStringValue("Row" + rowIndex);
                });
        final VirtualTable table = new VirtualTable(sourceIdentifiers[0], new SourceTableProperties(sources[0]));
        final VirtualTable mappedCols = table.map(new int[ 0 ], createRowKey);
        return mappedCols.append(table.filterColumns(2));
    }

    public static VirtualTable vtRowIndexToRowKeyMap() {
        return vtRowIndexToRowKeyMap(new UUID[]{randomUUID()}, dataMinimal());
    }

    @Test
    public void testRowIndexToRowKeyMap() {
        final ColumnarSchema expectedSchema = ColumnarSchema.of(STRING, STRING);
        final Object[][] expectedValues = new Object[][]{ //
                new Object[]{"Row0", "First"}, //
                new Object[]{"Row1", "Second"}, //
                new Object[]{"Row2", "Third"}, //
                new Object[]{"Row3", "Fourth"}, //
                new Object[]{"Row4", "Fifth"} //
        };
        testTransformedTable(expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataMinimal, VirtualTableTests::vtRowIndexToRowKeyMap);
        testTransformedTableLookahead(true, VirtualTableTests::dataMinimal, VirtualTableTests::vtRowIndexToRowKeyMap);
        testTransformedTableRandomAccess(true, expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataMinimal, VirtualTableTests::vtRowIndexToRowKeyMap);
    }



    public static VirtualTable vtNoInputsMap(final UUID[] sourceIdentifiers, final RowAccessible[] sources) {
        final MapperFactory constant = MapperFactory.of(ColumnarSchema.of(INT), (inputs, outputs) -> {
            MapTransformUtils.verify(inputs, 0, outputs, 1);
            final IntAccess.IntWriteAccess o = (IntAccess.IntWriteAccess)outputs[0];
            return () -> o.setIntValue(12345);
        });
        final VirtualTable table =
                new VirtualTable(sourceIdentifiers[0], new SourceTableProperties(sources[0])).slice(0, 7);
        final VirtualTable mappedCols = table.map(new int[]{}, constant);
        return table
                .keepOnlyColumns(0,1)
                .append(List.of(mappedCols))
                .slice(0,7);
    }

    public static VirtualTable vtNoInputsMap() {
        return vtNoInputsMap(new UUID[]{randomUUID()}, dataNoInputsMap());
    }

    public static RowAccessible[] dataNoInputsMap() {
        return dataSimpleMap();
    }

    @Test
    public void testNoInputsMap() {
        final ColumnarSchema expectedSchema = ColumnarSchema.of(INT, STRING, INT);
        final Object[][] expectedValues = new Object[][]{ //
                new Object[]{1, "First", 12345}, //
                new Object[]{2, "Second", 12345}, //
                new Object[]{3, "Third", 12345}, //
                new Object[]{4, "Fourth", 12345}, //
                new Object[]{5, "Fifth", 12345}, //
                new Object[]{6, "Sixth", 12345}, //
                new Object[]{7, "Seventh", 12345} //
        };
        testTransformedTable(expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataNoInputsMap, VirtualTableTests::vtNoInputsMap);
        testTransformedTableLookahead(true, VirtualTableTests::dataNoInputsMap, VirtualTableTests::vtNoInputsMap);
        testTransformedTableRandomAccess(true, expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataNoInputsMap, VirtualTableTests::vtNoInputsMap);
    }



    public static VirtualTable vtAppendTwice(final UUID[] sourceIdentifiers, final RowAccessible[] sources) {
        final VirtualTable t1 = new VirtualTable(sourceIdentifiers[0], new SourceTableProperties(sources[0]));
        final VirtualTable t2 = new VirtualTable(sourceIdentifiers[1], new SourceTableProperties(sources[1]));
        return t1.append(t2).append(t2);
    }

    public static VirtualTable vtAppendTwice() {
        return vtAppendTwice(new UUID[]{randomUUID(), randomUUID()}, dataAppendTwice());
    }

    public static RowAccessible[] dataAppendTwice() {
        final ColumnarSchema schema1 = ColumnarSchema.of(INT);
        final Object[][] values1 = new Object[][]{ //
                new Object[]{1}, //
                new Object[]{2}, //
                new Object[]{3}, //
                new Object[]{4}, //
                new Object[]{5} //
        };
        final ColumnarSchema schema2 = ColumnarSchema.of(INT);
        final Object[][] values2 = new Object[][]{ //
                new Object[]{11}, //
                new Object[]{12} //
        };
        return new RowAccessible[] {
                RowAccessiblesTestUtils.createRowAccessibleFromRowWiseValues(schema1, values1),
                RowAccessiblesTestUtils.createRowAccessibleFromRowWiseValues(schema2, values2),
        };
    }

    @Test
    public void testAppendTwice() {
        final ColumnarSchema expectedSchema = ColumnarSchema.of(INT, INT, INT);
        final Object[][] expectedValues = new Object[][]{ //
                new Object[]{1, 11,   11}, //
                new Object[]{2, 12,   12}, //
                new Object[]{3, null, null}, //
                new Object[]{4, null, null}, //
                new Object[]{5, null, null}, //
        };
        testTransformedTable(expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataAppendTwice, VirtualTableTests::vtAppendTwice);
        testTransformedTableLookahead(true, VirtualTableTests::dataAppendTwice, VirtualTableTests::vtAppendTwice);
        testTransformedTableRandomAccess(true, expectedSchema, expectedValues, expectedValues.length, VirtualTableTests::dataAppendTwice, VirtualTableTests::vtAppendTwice);
    }
}
