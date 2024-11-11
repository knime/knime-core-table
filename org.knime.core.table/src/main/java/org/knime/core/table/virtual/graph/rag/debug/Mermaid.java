package org.knime.core.table.virtual.graph.rag.debug;

import java.io.FileWriter;
import java.io.IOException;

import org.knime.core.table.virtual.graph.rag.BranchGraph;
import org.knime.core.table.virtual.graph.rag.TableTransformGraph;

/**
 * Visualize {@code TableTransformGraph} using <a href=https://mermaid-js.github.io>mermaid</a>.
 */
public class Mermaid {

    private static final boolean darkMode = true;

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
        sb.append(graph);
        sb.append("</div><br/>\n");
    }

    public void append(final String html) {
        sb.append(html);
    }

    public void save(final String filename) {
        try (FileWriter w = new FileWriter(filename)) {
            w.write(header + sb + footer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static final String header = """
            <!DOCTYPE html>
            <html lang="en">
            <head>
              <meta charset="UTF-8">
              <title>virtual table</title>
              <style>
            """ + (darkMode ? """
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

    private static final String footer = """
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
                case DATA -> (darkMode ? "blue" : "#8888FF,anything");
                case CONTROL -> (darkMode ? "red" : "#FF8888,anything");
                case EXECUTION -> (darkMode ? "lime" : "lime");
            });
            sb.append(";\n");
            ++edgeId;
        }
        return sb.toString();
    }
}
