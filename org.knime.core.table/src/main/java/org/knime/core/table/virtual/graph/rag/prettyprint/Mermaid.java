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
 *
 * History
 *   3 Dec 2024 (pietzsch): created
 */
package org.knime.core.table.virtual.graph.rag.prettyprint;

import java.io.FileWriter;
import java.io.IOException;

import org.knime.core.table.virtual.graph.rag.BranchGraph;
import org.knime.core.table.virtual.graph.rag.TableTransformGraph;

/**
 * Visualize {@code TableTransformGraph} using <a href=https://mermaid-js.github.io>mermaid</a>.
 */
public class Mermaid {

    private static final boolean DARK_MODE = true;

    private final StringBuilder sb;

    public Mermaid() {
        sb = new StringBuilder();
    }

    public void append(final String title, final TableTransformGraph graph) {
        append(title, null, graph);
    }

    public void append(final String title, final String description, final TableTransformGraph graph) {
        append(title, description, mermaid(new DependencyGraph(graph)));
    }

    public void append(final String title, final BranchGraph graph) {
        append(title, null, graph);
    }

    public void append(final String title, final String description, final BranchGraph graph) {
        append(title, description, mermaid(new DependencyGraph(graph)));
    }

    private void append(final String title, final String description, final String graph) {
        if (title != null) {
            sb.append("<h3>").append(title).append("</h3>\n");
        }
        if (description != null) {
            sb.append(description).append("<br/>\n");
        }
        sb.append("<div class=\"mermaid\">\n");
        sb.append("%%{init: {'theme': 'base', 'themeVariables': { ");
        if (DARK_MODE) {
            sb.append("'background' : '#2B2B2B', ");
            sb.append("'primaryColor': '#444444', ");
            sb.append("'darkMode': 'true'");
        } else {
            sb.append("'background' : '#AAAAAA', ");
        }
        sb.append("}}}%%\n");
        sb.append(graph);
        sb.append("</div><br/>\n");
    }

    public void append(final String html) {
        sb.append(html);
    }

    public void save(final String filename) throws IOException {
        try (FileWriter w = new FileWriter(filename)) {
            w.write(HEADER + sb + FOOTER);
        }
    }

    private static final String HEADER = """
            <!DOCTYPE html>
            <html lang="en">
            <head>
              <meta charset="UTF-8">
              <title>virtual table</title>
              <style>
            """ + (DARK_MODE ? """
                body {
                  color: #EEEEEE;
                  background-color:#2B2B2B;
                }
            """ : """
                body {
                  color: #000000;
                  background-color:#FFFFFF;
                }
            """) + """
                h3 {
                  margin-bottom: 5px
                }
              </style>
            </head>
            <body>
              <script src="https://cdn.jsdelivr.net/npm/mermaid/dist/mermaid.min.js"></script>
              <script>
                mermaid.initialize({ startOnLoad: true });
              </script>
            """;

    private static final String FOOTER = """
            </body>
            </html>
            """;

    private static String mermaid(final DependencyGraph depGraph) {
        final var sb = new StringBuilder("graph BT\n");
        for (var node : depGraph.nodes) {
            final String name = "<" + node.id() + "> " + node.spec();
            sb.append("  " + node.id() + "(\"" + name + "\")\n");
        }
        int edgeId = 0;
        for (var edge : depGraph.edges) {
            sb.append("  " + edge.from().id() + "--> " + edge.to().id() + "\n");
            sb.append("  linkStyle " + edgeId + " stroke:");
            sb.append(switch (edge.type()) {
                case DATA -> (DARK_MODE ? "blue" : "#8888FF,anything");
                case CONTROL -> (DARK_MODE ? "red" : "#FF8888,anything");
                case EXECUTION -> "lime";
            });
            sb.append(";\n");
            ++edgeId;
        }
        return sb.toString();
    }
}
