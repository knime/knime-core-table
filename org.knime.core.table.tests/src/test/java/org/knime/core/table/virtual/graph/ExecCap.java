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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.knime.core.table.cursor.Cursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.row.Selection;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.VirtualTable;
import org.knime.core.table.virtual.graph.cap.CursorAssemblyPlan;
import org.knime.core.table.virtual.graph.exec.CapExecutor;
import org.knime.core.table.virtual.graph.rag.BranchGraph;
import org.knime.core.table.virtual.graph.rag.BuildCap;
import org.knime.core.table.virtual.graph.rag.TableTransformGraph;
import org.knime.core.table.virtual.graph.rag.TableTransformUtil;
import org.knime.core.table.virtual.graph.util.ReadAccessUtils;
import org.knime.core.table.virtual.spec.SourceTableProperties.CursorType;

public class ExecCap {

    public static void main(String[] args) {

//        final UUID[] sourceIdentifiers = createSourceIds(1);
//        final RowAccessible[] sourceAccessibles = VirtualTableTests.dataMinimal();
//        final VirtualTable table = VirtualTableTests.vtMinimal(sourceIdentifiers, sourceAccessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(2);
//        final RowAccessible[] sourceAccessibles = VirtualTableTests.dataAppend();
//        final VirtualTable table = VirtualTableTests.vtAppend(sourceIdentifiers, sourceAccessibles);

//        final UUID[] sourceIdentifiers = createSourceIds(1);
//        final RowAccessible[] sourceAccessibles = VirtualTableTests.dataMapsAndFilters();
//        final VirtualTable table = VirtualTableTests.vtMapsAndFilters(sourceIdentifiers, sourceAccessibles);


        final UUID[] sourceIdentifiers = createSourceIds(1);
        final RowAccessible[] sourceAccessibles = VirtualTableTests.dataLinear();
        final VirtualTable table = VirtualTableTests.vtLinear(sourceIdentifiers, sourceAccessibles).slice(2, 4).slice(0, 1);


        final Map<UUID, RowAccessible> uuidRowAccessibleMap = new HashMap<>();
        for (int i = 0; i < sourceIdentifiers.length; ++i) {
            uuidRowAccessibleMap.put(sourceIdentifiers[i], sourceAccessibles[i]);
        }

        // create TableTransformGraph
        var tableTransformGraph = new TableTransformGraph(table.getProducingTransform());
        TableTransformUtil.optimize(tableTransformGraph);
        System.out.println(tableTransformGraph);
        System.out.println();

        // TableTransformGraph properties
        final CursorType cursorType = tableTransformGraph.supportedCursorType();
        final ColumnarSchema schema = tableTransformGraph.createSchema();
        final long numRows = tableTransformGraph.numRows();
        System.out.println("inferred     cursorType = " + cursorType);
        System.out.println("inferred schema = " + schema);
        System.out.println("inferred numRows = " + numRows);
        System.out.println();


        // create CAP
        BranchGraph depGraph = new BranchGraph(tableTransformGraph);
        CursorAssemblyPlan cap = BuildCap.createCursorAssemblyPlan(depGraph);

        // create RowAccessible
        RowAccessible rows = CapExecutor.createRowAccessible(tableTransformGraph, schema, cursorType, uuidRowAccessibleMap);

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

        // use RowAccessible with column selection
        try (final Cursor<ReadAccessRow> cursor = rows.createCursor(Selection.all().retainColumns(0))) {
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

    }

    private static UUID[] createSourceIds(final int n) {
        final UUID[] ids = new UUID[n];
        Arrays.setAll(ids, i -> UUID.randomUUID());
        return ids;
    }
}
