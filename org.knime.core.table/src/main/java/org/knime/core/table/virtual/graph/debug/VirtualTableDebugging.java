package org.knime.core.table.virtual.graph.debug;

import static org.knime.core.table.virtual.graph.rag.RagEdgeType.FLATTENED_ORDER;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.knime.core.table.virtual.graph.rag.RagEdge;
import org.knime.core.table.virtual.graph.rag.RagGraph;
import org.knime.core.table.virtual.graph.rag.RagNode;

public class VirtualTableDebugging {

    public static boolean enableRagGraphLogging = true;

    public interface Logger extends AutoCloseable {

        void append(String text);

        void appendRagGraph(String title, RagGraph graph);

        void appendRagGraph(String title, String description, RagGraph graph);

        void appendOrderedRagGraph(String title, String description, RagGraph graph, List<RagNode> order);

        @Override
        void close();
    }

    public static Logger createLogger() {
        return enableRagGraphLogging ? new MermaidLogger() : new NullLogger();
    }

    public static class NullLogger implements Logger {

        @Override
        public void append(String text) {}

        @Override
        public void appendRagGraph(String title, RagGraph graph) {}

        @Override
        public void appendRagGraph(String title, String description, RagGraph graph) {}

        @Override
        public void appendOrderedRagGraph(String title, String description, RagGraph graph, List<RagNode> order) {}

        @Override
        public void close() {}
    }

    public static class MermaidLogger implements Logger {

        private static final AtomicInteger nextIndex = new AtomicInteger();

        private final int index;

        private final Mermaid mermaid;

        public MermaidLogger() {
            index = nextIndex.getAndIncrement();
            mermaid = new Mermaid();
            append("Log #" + index);
            append(printStackTrace(3, -1));
        }

        @Override
        public void append(final String text) {
            mermaid.append("<pre>" + text + "</pre>\n");
        }

        @Override
        public void appendRagGraph(final String title, final RagGraph graph) {
            mermaid.append(title, graph);
        }

        @Override
        public void appendRagGraph(final String title, final String description, final RagGraph graph) {
            mermaid.append(title, description, graph);
        }

        @Override
        public void appendOrderedRagGraph(String title, String description, RagGraph graph, List<RagNode> order) {
            final List<RagEdge> edges = new ArrayList<>();
            for (int i = 0; i < order.size() - 1; i++)
                edges.add(graph.addEdge(order.get(i), order.get(i + 1), FLATTENED_ORDER));
            appendRagGraph(title, description, graph);
            edges.forEach(graph::remove);
        }

        @Override
        public void close() {
            // TODO: Make this configurable ...
            var filename = String.format("/Users/pietzsch/git/mermaid/irgraph%03d.html", index);
            System.out.println("Writing Mermaid output to " + filename);
            mermaid.save(filename);
        }

        private static String printStackTrace( int startDepth, int maxDepth, String ... unlessContains )
        {
            final StackTraceElement[] trace = Thread.currentThread().getStackTrace();

            final var sb = new StringBuilder();
            final int len = ( maxDepth < 0 )
                    ? trace.length
                    : Math.min( startDepth + maxDepth, trace.length );
            for ( int i = startDepth; i < len; ++i )
            {
                final String prefix = ( i == startDepth ) ? "" : "    at ";
                sb.append(prefix).append(trace[i].toString()).append("\n");
            }

            return sb.toString();
        }
    }
}
