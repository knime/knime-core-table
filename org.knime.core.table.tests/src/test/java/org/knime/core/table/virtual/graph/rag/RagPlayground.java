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
package org.knime.core.table.virtual.graph.rag;

import org.knime.core.table.virtual.VirtualTable;
import org.knime.core.table.virtual.graph.VirtualTableTests;
import org.knime.core.table.virtual.graph.cap.CapNode;
import org.knime.core.table.virtual.graph.cap.CursorAssemblyPlan;
import org.knime.core.table.virtual.graph.rag3.BranchGraph;
import org.knime.core.table.virtual.graph.rag3.BuildCap;
import org.knime.core.table.virtual.graph.rag3.TableTransformGraph;
import org.knime.core.table.virtual.graph.rag3.TableTransformUtil;
import org.knime.core.table.virtual.graph.rag3.debug.DependencyGraph;
import org.knime.core.table.virtual.graph.rag3.debug.Mermaid;

public class RagPlayground {

    public static void main(String[] args) {
//        final VirtualTable table = VirtualTableTests.vtMinimal();
//        final VirtualTable table = VirtualTableTests.vtAppend();
//        final VirtualTable table = VirtualTableTests.vtAppendAndSlice();
//        final VirtualTable table = VirtualTableTests.vtMapsAndFilters();
//        final VirtualTable table = VirtualTableTests.vtLinear().slice(2,4).slice(0,1);
//        final VirtualTable table = VirtualTableTests.vtSimpleAppendMap();
        final VirtualTable table = VirtualTableTests.vtObserve();

        var tableTransformGraph = new TableTransformGraph(table.getProducingTransform());
        System.out.println("tableTransformGraph = " + tableTransformGraph);

        TableTransformUtil.pruneAccesses(tableTransformGraph);

        var dependencyGraph = new DependencyGraph(tableTransformGraph);
        System.out.println("graph = " + dependencyGraph);

        var mermaid = new Mermaid();
        mermaid.append("<pre>" + dependencyGraph + "</pre>");
        mermaid.append("SpecGraph", tableTransformGraph);

        while(TableTransformUtil.mergeSlices(tableTransformGraph)) {
        }
        mermaid.append("SpecGraph", tableTransformGraph);

        BranchGraph depGraph = new BranchGraph(tableTransformGraph);
        System.out.println("depGraph = " + depGraph);
        mermaid.append("BranchGraph", depGraph);
        mermaid.save("/Users/pietzsch/git/mermaid/b_graph.html");

        CursorAssemblyPlan cap = BuildCap.createCursorAssemblyPlan(depGraph);
        System.out.println();
        System.out.println("cap = ");
        for (CapNode node : cap.nodes()) {
            System.out.println("<" + node.index() + "> " + node);
        }
    }
}
