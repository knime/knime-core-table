package org.knime.core.table.virtual.graph;

import java.io.IOException;

import org.knime.core.table.access.BufferedAccesses;
import org.knime.core.table.access.BufferedAccesses.BufferedAccessRow;
import org.knime.core.table.cursor.Cursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.graph.util.ReadAccessUtils;

public class SplitterPlayground {

    public static void main(String[] args) {
        final RowAccessible input = VirtualTableExamples.dataConsecutiveRowFilters()[ 0 ];
        final ColumnarSchema schema = input.getSchema();

        System.out.println("schema = " + schema);
        System.out.println();

        final SplitSource source = new SplitSource(input);
        final int nThreads = 4;

        for (int t = 1; t <= nThreads; t++) {
            final int threadIndex = t;
            new Thread(() -> {
                try (final Cursor<ReadAccessRow> cursor = new SplitCursor(source)) {
                    while (cursor.forward()) {
                        final StringBuffer sb = new StringBuffer("[thread ").append(threadIndex).append("]: ");
                        for (int i = 0; i < cursor.access().size(); i++)
                            sb.append(ReadAccessUtils.toString(cursor.access().getAccess(i)) + ", ");
                        System.out.println(sb);
                        Thread.sleep((long)(Math.random() * 100));
                    }
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            }).start();
        }
    }

    static class SplitSource {

        private final ColumnarSchema schema;
        private final Cursor<ReadAccessRow> cursor;

        public SplitSource(final RowAccessible rows) {
            schema = rows.getSchema();
            cursor = rows.createCursor();
        }

        public ColumnarSchema getSchema() {
            return schema;
        }

        public synchronized boolean forward(final BufferedAccessRow buffer) {
            if (cursor.forward()) {
                buffer.setFrom(cursor.access());
                return true;
            }
            return false;
        }
    }

    static class SplitCursor implements Cursor<ReadAccessRow> {

        private final SplitSource source;

        private final BufferedAccessRow bufferedAccessRow;

        public SplitCursor(final SplitSource source) {
            this.source = source;
            this.bufferedAccessRow = BufferedAccesses.createBufferedAccessRow(source.getSchema());
        }

        @Override
        public ReadAccessRow access() {
            return bufferedAccessRow;
        }

        @Override
        public boolean forward() {
            return source.forward(bufferedAccessRow);
        }

        @Override
        public void close() throws IOException {
            // TODO?
        }
    }
}
