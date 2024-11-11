package org.knime.core.table.virtual.graph.debug;

import org.knime.core.table.virtual.graph.rag.BranchGraph;
import org.knime.core.table.virtual.graph.rag.TableTransformGraph;
import org.knime.core.table.virtual.graph.rag.debug.Mermaid;

import java.util.concurrent.atomic.AtomicInteger;

public class VirtualTableDebugging {

    public static boolean enableRagGraphLogging = true;

    public interface Logger extends AutoCloseable {

        void append(String text);

        void appendGraph(String title, TableTransformGraph graph);

        void appendGraph(String title, String description, TableTransformGraph graph);

        void appendGraph(String title, String description, BranchGraph graph);

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
        public void appendGraph(String title, TableTransformGraph graph) {}

        @Override
        public void appendGraph(String title, String description, TableTransformGraph graph) {}

        @Override
        public void appendGraph(String title, String description, BranchGraph graph) {}

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
        public void appendGraph(final String title, final TableTransformGraph graph) {
            mermaid.append(title, graph);
        }

        @Override
        public void appendGraph(final String title, final String description, final TableTransformGraph graph) {
            mermaid.append(title, description, graph);
        }

        @Override
        public void appendGraph(String title, String description, BranchGraph graph) {
            mermaid.append(title, description, graph);
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
