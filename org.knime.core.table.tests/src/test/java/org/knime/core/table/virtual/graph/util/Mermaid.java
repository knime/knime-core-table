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
        final var sb = new StringBuffer("graph TD\n");
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
        final var sb = new StringBuffer("<!DOCTYPE html>\n");
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

    private final StringBuffer sb;

    public Mermaid() {
        sb = new StringBuffer("<!DOCTYPE html>\n");
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
