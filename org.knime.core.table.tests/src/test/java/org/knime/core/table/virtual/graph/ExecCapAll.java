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

import static org.knime.core.table.RowAccessiblesTestUtils.toLookahead;
import static org.knime.core.table.virtual.graph.exec.CapExecutor.createRowAccessible;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.knime.core.table.cursor.Cursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.virtual.VirtualTable;
import org.knime.core.table.virtual.graph.rag.TableTransformGraph;
import org.knime.core.table.virtual.graph.rag.TableTransformUtil;
import org.knime.core.table.virtual.graph.util.ReadAccessUtils;

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

        final TableTransformGraph graph = new TableTransformGraph(table.getProducingTransform());
        TableTransformUtil.optimize(graph);
        final RowAccessible rows = createRowAccessible(graph, uuidRowAccessibleMap);

        System.out.println(exampleName);
        System.out.println("------------------------");
        System.out.println("size = " + rows.size());
        System.out.println(table.getSchema());
        System.out.println("------------------------");
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
        System.out.println();
        System.out.println();
    }

    private static UUID[] createSourceIds(final int n) {
        final UUID[] ids = new UUID[n];
        Arrays.setAll(ids, i -> UUID.randomUUID());
        return ids;
    }
}
