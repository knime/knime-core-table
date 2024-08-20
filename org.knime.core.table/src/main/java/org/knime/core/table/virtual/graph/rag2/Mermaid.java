package org.knime.core.table.virtual.graph.rag2;

import java.io.FileWriter;
import java.io.IOException;

import org.knime.core.table.virtual.graph.rag2.SpecGraphBuilder.SpecGraph;

/**
 * Visualize {@code RagGraph} using <a href=https://mermaid-js.github.io>mermaid</a>.
 */
public class Mermaid {

    private static final boolean darkMode = false;

    private static final boolean hideMissingValuesSource = false;

    private final StringBuilder sb;

    public Mermaid() {
        sb = new StringBuilder();
    }

    public void append(final String title, final SpecGraph graph) {
        append(title, null, graph);
    }

    public void append(final String title, final String description, final SpecGraph graph) {
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
        sb.append(SpecGraphBuilder.mermaid(graph, darkMode));
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
}
