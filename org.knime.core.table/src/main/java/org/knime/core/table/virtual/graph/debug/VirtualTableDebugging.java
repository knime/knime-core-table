package org.knime.core.table.virtual.graph.debug;

import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicInteger;

import org.knime.core.table.virtual.graph.rag.BranchGraph;
import org.knime.core.table.virtual.graph.rag.TableTransformGraph;
import org.knime.core.table.virtual.graph.rag.debug.Mermaid;

public class VirtualTableDebugging {

    static final boolean enableTableTransformGraphLogging = false;

    public interface Logger extends AutoCloseable {

        void append(String text);

        void appendGraph(String title, TableTransformGraph graph);

        void appendGraph(String title, String description, TableTransformGraph graph);

        void appendGraph(String title, String description, BranchGraph graph);

        @Override
        void close();
    }

    public static Logger createLogger() {
        return enableTableTransformGraphLogging ? new MermaidLogger() : new NullLogger();
    }

    public static final class NullLogger implements Logger {

        @Override
        public void append(final String text) {
            // NOSONAR
        }

        @Override
        public void appendGraph(final String title, final TableTransformGraph graph) {
            // NOSONAR
        }

        @Override
        public void appendGraph(final String title, final String description, final TableTransformGraph graph) {
            // NOSONAR
        }

        @Override
        public void appendGraph(final String title, final String description, final BranchGraph graph) {
            // NOSONAR
        }

        @Override
        public void close() {
            // NOSONAR
        }
    }

    public static final class MermaidLogger implements Logger {

        private static final AtomicInteger nextIndex = new AtomicInteger();

        private final Mermaid mermaid;

        private final String filename;

        public MermaidLogger() {
            this(String.format("/Users/pietzsch/git/mermaid/irgraph%03d.html", nextIndex.getAndIncrement()));
        }

        public MermaidLogger(final String filename) {
            mermaid = new Mermaid();
            this.filename = filename;
            append("Log " + LocalDateTime.now());
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
        public void appendGraph(final String title, final String description, final BranchGraph graph) {
            mermaid.append(title, description, graph);
        }

        @Override
        public void close() {
            System.out.println("Writing Mermaid output to " + filename);
            mermaid.save(filename);
        }

        private static String printStackTrace(final int startDepth, final int maxDepth) {
            final StackTraceElement[] trace = Thread.currentThread().getStackTrace();

            final var sb = new StringBuilder();
            final int len = (maxDepth < 0) ? trace.length : Math.min(startDepth + maxDepth, trace.length);
            for (int i = startDepth; i < len; ++i) {
                final String prefix = (i == startDepth) ? "" : "    at ";
                sb.append(prefix).append(trace[i].toString()).append("\n");
            }

            return sb.toString();
        }
    }
}
