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
package org.knime.core.table.virtual.graph.util;

import static org.knime.core.table.virtual.graph.rag.RagEdgeType.SPEC;

import java.io.FileWriter;
import java.io.IOException;

import org.knime.core.table.virtual.graph.rag.RagEdge;
import org.knime.core.table.virtual.graph.rag.RagGraph;
import org.knime.core.table.virtual.graph.rag.RagNode;
import org.knime.core.table.virtual.graph.rag.RagNodeType;

/**
 * Visualize {@code IRGraph} using <a href=https://mermaid-js.github.io>mermaid</a>.
 */
public class Mermaid {

    private static final boolean darkMode = true;

    private static final boolean hideMissingValuesSource = false;

    public static void save(final String filename, final RagGraph graph) {
        try (FileWriter w = new FileWriter(filename)) {
            w.write(makePage(graph));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String mermaid(final RagGraph graph) {
        final var sb = new StringBuilder("graph TD\n");
        for (final RagNode node : graph.nodes()) {
            if (hideMissingValuesSource && node.type() == RagNodeType.MISSING)
                continue;
            final String name = "<" + node.id() + "> " + node.getTransformSpec().toString();
            sb.append("  " + node.id() + "(\"" + name + "\")\n");
        }
        int edgeId = 0;
        for (final RagEdge edge : graph.edges()) {
            if (edge.type() == SPEC)
                sb.append("  " + edge.getSource().id() + "--- " + edge.getTarget().id() + "\n");
            else
                sb.append("  " + edge.getSource().id() + "--> " + edge.getTarget().id() + "\n");
            switch (edge.type() )
            {
                case SPEC:
                    sb.append("  linkStyle " + edgeId + " stroke:" + //
                            (darkMode ? "#444444,anything" : "#DDDDDD,anything") + ";\n");
                    break;
                case DATA:
                    sb.append("  linkStyle " + edgeId + " stroke:" + //
                            (darkMode ? "blue" : "#8888FF,anything") + ";\n");
                    break;
                case EXEC:
                    sb.append("  linkStyle " + edgeId + " stroke:" + //
                            (darkMode ? "red" : "#FF8888,anything") + ";\n");
                    break;
                case ORDER:
                    sb.append("  linkStyle " + edgeId + " stroke:" + //
                            (darkMode ? "white" : "black") + ";\n");
                    break;
                case FLATTENED_ORDER:
                    sb.append("  linkStyle " + edgeId + " stroke:" + //
                            (darkMode ? "lime" : "lime") + ";\n");
                    break;
            }
            ++edgeId;
        }
        return sb.toString();
    }

    private static String makePage(final RagGraph graph) {
        final var sb = new StringBuilder("<!DOCTYPE html>\n");
        sb.append("<html lang=\"en\">\n");
        sb.append("<head>\n");
        sb.append("<meta charset=\"UTF-8\">\n");
        sb.append("<title>virtual table</title>\n");
        sb.append("</head>\n");
        sb.append("<body style=\"background-color:#2B2B2B;\">\n");
        sb.append("<script src=\"https://cdn.jsdelivr.net/npm/mermaid/dist/mermaid.min.js\"></script>\n");
        sb.append("<script>\n");
        sb.append("mermaid.initialize({ startOnLoad: true });\n");
        sb.append("</script>\n");
        sb.append("<div class=\"mermaid\">\n");
        sb.append("%%{init: {'theme': 'base', 'themeVariables': { ");
        if (darkMode) {
            sb.append("'background' : '#2B2B2B', ");
            sb.append("'primaryColor': '#444444', ");
            sb.append("'darkMode': 'true'");
        } else {
            sb.append("'background' : '#EEEEEE', ");
            sb.append("'primaryColor': '#004400', ");
            sb.append("'darkMode': 'false'");
        }
        sb.append("}}}%%\n");
        sb.append(mermaid(graph));
        sb.append("</div>\n");
        sb.append("</body>\n");
        sb.append("</html>\n");
        return sb.toString();
    }

    private final StringBuilder sb;

    public Mermaid() {
        sb = new StringBuilder("<!DOCTYPE html>\n");
        sb.append("<html lang=\"en\">\n");
        sb.append("<head>\n");
        sb.append("<meta charset=\"UTF-8\">\n");
        sb.append("<title>virtual table</title>\n");
        sb.append("<style>\n");
        sb.append("body {\n");
        if (darkMode) {
            sb.append("  color: #EEEEEE;\n");
            sb.append("  background-color:#2B2B2B;\n");
        } else {
            sb.append("  color: #000000;\n");
            sb.append("  background-color:#FFFFFF;\n");
        }
        sb.append("}\n");
        sb.append("h3 {\n");
        sb.append("  margin-bottom: 5px\n");
        sb.append("}\n");
        sb.append("</style>\n");
        sb.append("</head>\n");
        sb.append("<body>\n");
        sb.append("<script src=\"https://cdn.jsdelivr.net/npm/mermaid/dist/mermaid.min.js\"></script>\n");
        sb.append("<script>\n");
        sb.append("mermaid.initialize({ startOnLoad: true });\n");
        sb.append("</script>\n");
    }

    public void append(final String title, final RagGraph graph) {
        append(title, null, graph);
    }

    public void append(final String title, final String description, final RagGraph graph) {
        if (title != null) {
            sb.append("<h3>").append(title).append("</h3>\n");
        }
        if (description != null) {
            sb.append(description).append("<br/>\n");
        }
        sb.append("<div class=\"mermaid\">\n");
        sb.append("%%{init: {'theme': 'base', 'themeVariables': { ");
        if (darkMode) {
            sb.append("'background' : '#2B2B2B', ");
            sb.append("'primaryColor': '#444444', ");
            sb.append("'darkMode': 'true'");
        } else {
            sb.append("'background' : '#AAAAAA', ");
//            sb.append("'primaryColor': '#44FF44', ");
//            sb.append("'darkMode': 'false'");
        }
        sb.append("}}}%%\n");
        sb.append(mermaid(graph));
        sb.append("</div><br/>\n");
    }

    public void save(final String filename) {
        try (FileWriter w = new FileWriter(filename)) {
            w.write(sb.toString() + "</body>\n</html>\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
