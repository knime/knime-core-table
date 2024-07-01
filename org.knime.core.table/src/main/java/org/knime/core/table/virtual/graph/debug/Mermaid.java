package org.knime.core.table.virtual.graph.debug;

import static org.knime.core.table.virtual.graph.rag.RagEdgeType.SPEC;

import java.io.FileWriter;
import java.io.IOException;

import org.knime.core.table.virtual.graph.rag.RagEdge;
import org.knime.core.table.virtual.graph.rag.RagGraph;
import org.knime.core.table.virtual.graph.rag.RagNode;
import org.knime.core.table.virtual.graph.rag.RagNodeType;

/**
 * Visualize {@code RagGraph} using <a href=https://mermaid-js.github.io>mermaid</a>.
 */
public class Mermaid {

    private static final boolean darkMode = true;

    private static final boolean hideMissingValuesSource = false;

    private final StringBuilder sb;

    public Mermaid() {
        sb = new StringBuilder();
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

        sb.append("<pre>");
        graph.nodes().forEach(node -> sb.append(node).append("\n"));
        sb.append("</pre>\n");
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

    private static String mermaid(final RagGraph graph) {
        final var sb = new StringBuilder("graph TD\n");
        for (final RagNode node : graph.nodes()) {
            if (hideMissingValuesSource && node.type() == RagNodeType.MISSING) {
                continue;
            }
            final String name = "<" + node.id() + "> " + node.getTransformSpec().toString();
            sb.append("  " + node.id() + "(\"" + name + "\")\n");
        }
        int edgeId = 0;
        for (final RagEdge edge : graph.edges()) {
            if (edge.type() == SPEC) {
                sb.append("  " + edge.getSource().id() + "--- " + edge.getTarget().id() + "\n");
            } else {
                sb.append("  " + edge.getSource().id() + "--> " + edge.getTarget().id() + "\n");
            }
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
}
