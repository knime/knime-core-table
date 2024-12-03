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
package org.knime.core.table.virtual.graph.debug;

import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicInteger;

import org.knime.core.table.virtual.graph.rag.BranchGraph;
import org.knime.core.table.virtual.graph.rag.TableTransformGraph;
import org.knime.core.table.virtual.graph.rag.debug.Mermaid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VirtualTableDebugging {

    static final boolean ENABLE_TABLE_TRANSFORM_GRAPH_LOGGING = false;

    public interface TableTransformGraphLogger extends AutoCloseable {

        void append(String text);

        void appendGraph(String title, TableTransformGraph graph);

        void appendGraph(String title, String description, TableTransformGraph graph);

        void appendGraph(String title, String description, BranchGraph graph);

        @Override
        void close();
    }

    public static TableTransformGraphLogger createLogger() {
        return ENABLE_TABLE_TRANSFORM_GRAPH_LOGGING ? new MermaidLogger() : new NullLogger();
    }

    public static final class NullLogger implements TableTransformGraphLogger {

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

    public static final class MermaidLogger implements TableTransformGraphLogger {

        private static final Logger LOGGER = LoggerFactory.getLogger(MermaidLogger.class);

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
            LOGGER.info("Writing Mermaid output to " + filename);
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
