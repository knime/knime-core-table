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
                try (final SplitCursor cursor = new SplitCursor(source)) {
                    while (cursor.forward()) {
                        final long millis = (long)(Math.random() * 100);
                        Thread.sleep(millis);
                        final StringBuffer sb = new StringBuffer("[thread ").append(threadIndex).append("]: ");
                        sb.append("row ").append(cursor.rowIndex()).append(", ");
                        for (int i = 0; i < cursor.access().size(); i++)
                            sb.append(ReadAccessUtils.toString(cursor.access().getAccess(i))).append(", ");
                        sb.append("millis=").append(millis);
                        System.out.println(sb);
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
        private long rowIndex;

        public SplitSource(final RowAccessible rows) {
            schema = rows.getSchema();
            cursor = rows.createCursor();
            rowIndex = 0;
        }

        public ColumnarSchema getSchema() {
            return schema;
        }

        public synchronized boolean forward(final SplitCursor splitCursor) {
            if (cursor.forward()) {
                splitCursor.bufferedAccessRow.setFrom(cursor.access());
                splitCursor.rowIndex = rowIndex++;
                return true;
            }
            return false;
        }
    }

    static class SplitCursor implements Cursor<ReadAccessRow> {

        private final SplitSource source;
        private final BufferedAccessRow bufferedAccessRow;
        private long rowIndex;

        public SplitCursor(final SplitSource source) {
            this.source = source;
            this.bufferedAccessRow = BufferedAccesses.createBufferedAccessRow(source.getSchema());
        }

        @Override
        public ReadAccessRow access() {
            return bufferedAccessRow;
        }

        public long rowIndex() {
            return rowIndex;
        }

        @Override
        public boolean forward() {
            return source.forward(this);
        }

        @Override
        public void close() throws IOException {
            // TODO?
        }
    }

}
