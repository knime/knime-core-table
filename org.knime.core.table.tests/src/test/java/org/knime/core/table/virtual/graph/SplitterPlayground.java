package org.knime.core.table.virtual.graph;

import static org.knime.core.table.schema.DataSpecs.BOOLEAN;
import static org.knime.core.table.schema.DataSpecs.DOUBLE;
import static org.knime.core.table.schema.DataSpecs.INT;
import static org.knime.core.table.schema.DataSpecs.STRING;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.knime.core.table.RowAccessiblesTestUtils;
import org.knime.core.table.access.BooleanAccess;
import org.knime.core.table.access.BufferedAccesses;
import org.knime.core.table.access.BufferedAccesses.BufferedAccessRow;
import org.knime.core.table.cursor.Cursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.graph.util.ReadAccessUtils;

public class SplitterPlayground {

    public static RowAccessible[] dataSplitter() {
        final ColumnarSchema schema = ColumnarSchema.of(INT, STRING, DOUBLE, BOOLEAN);
        final Object[][] values = new Object[][]{ //
                new Object[]{ 0, "str00",  0.0, true}, //
                new Object[]{ 1, "str01",  1.0, true}, //
                new Object[]{ 2, "str02",  2.0, true}, //
                new Object[]{ 3, "str03",  3.0, false}, //
                new Object[]{ 4, "str04",  4.0, true}, //
                new Object[]{ 5, "str05",  5.0, false}, //
                new Object[]{ 6, "str06",  6.0, true}, //
                new Object[]{ 7, "str07",  7.0, true}, //
                new Object[]{ 8, "str08",  8.0, true}, //
                new Object[]{ 9, "str09",  9.0, true}, //
                new Object[]{10, "str10", 10.0, false}, //
                new Object[]{11, "str11", 11.0, false}, //
                new Object[]{12, "str12", 12.0, true}, //
                new Object[]{13, "str13", 13.0, true}, //
                new Object[]{14, "str14", 14.0, true}, //
                new Object[]{15, "str15", 15.0, false}, //
                new Object[]{16, "str16", 16.0, true}, //
                new Object[]{17, "str17", 17.0, true}, //
                new Object[]{18, "str18", 18.0, true}, //
                new Object[]{19, "str19", 19.0, true}, //
                new Object[]{20, "str20", 20.0, true}, //
                new Object[]{21, "str21", 21.0, false}, //
                new Object[]{22, "str22", 22.0, true}, //
                new Object[]{23, "str23", 23.0, false}, //
                new Object[]{24, "str24", 24.0, false}, //
                new Object[]{25, "str25", 25.0, false}, //
                new Object[]{26, "str26", 26.0, false}, //
                new Object[]{27, "str27", 27.0, false}, //
                new Object[]{28, "str28", 28.0, false}, //
                new Object[]{29, "str29", 29.0, true}, //
                new Object[]{30, "str30", 30.0, false}, //
                new Object[]{31, "str31", 31.0, true}, //
                new Object[]{32, "str32", 32.0, true}, //
                new Object[]{33, "str33", 33.0, false}, //
                new Object[]{34, "str34", 34.0, false}, //
                new Object[]{35, "str35", 35.0, true}, //
                new Object[]{36, "str36", 36.0, true}, //
                new Object[]{37, "str37", 37.0, true}, //
                new Object[]{38, "str38", 38.0, false}, //
                new Object[]{39, "str39", 39.0, true}, //
        };
        return new RowAccessible[]{RowAccessiblesTestUtils.createRowAccessibleFromRowWiseValues(schema, values)};
    }


    public static void main(String[] args) {
        final RowAccessible input = dataSplitter()[ 0 ];
        final ColumnarSchema schema = input.getSchema();

        System.out.println("schema = " + schema);
        System.out.println();

        final SplitSource source = new SplitSource(input);
        final int nThreads = 4;

        RowConsumer consumer = new RowConsumer(4, source.getSchema());

        for (int t = 1; t <= nThreads; t++) {
            final int threadIndex = t;
            new Thread(() -> {
                try (final SplitCursor cursor = new SplitCursor(source)) {
                    while (cursor.forward()) {
                        final long millis = (long)(Math.random() * 100);
                        Thread.sleep(millis);

                        StringBuffer sb = new StringBuffer("[thread ").append(threadIndex).append("]: ");
                        sb.append("row ").append(cursor.rowIndex()).append(", ");
                        for (int i = 0; i < cursor.access().size(); i++)
                            sb.append(ReadAccessUtils.toString(cursor.access().getAccess(i))).append(", ");
                        sb.append("millis=").append(millis);
//                        System.out.println(sb);

                        final boolean skip = !((BooleanAccess.BooleanReadAccess)cursor.access().getAccess(3)).getBooleanValue();
                        if(skip)
                            consumer.skip(cursor.rowIndex());
                        else
                            consumer.accept(cursor.rowIndex(), cursor.access());

                        sb = new StringBuffer("[thread ").append(threadIndex).append("]: ");
                        sb.append("row ").append(cursor.rowIndex()).append(skip ? " skipped" : " submitted");
//                        System.out.println(sb);
                    }
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            }).start();
        }

        while (true) {
            try {
                BufferedAccessRow row = consumer.queue.take();
                StringBuffer sb = new StringBuffer("[CONSUMER]: ");
                for (int i = 0; i < row.size(); i++)
                    sb.append(ReadAccessUtils.toString(row.getAccess(i))).append(", ");
                System.out.println(sb);
                consumer.buffers.put(row);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    static class RowConsumer {

        private final RowBufferQueue<BufferedAccessRow> buffers;
        private final BlockingQueue<BufferedAccessRow> queue;
        private final RowOrderList<BufferedAccessRow> order;

        public RowConsumer(final int numBuffers, final ColumnarSchema schema)
        {
            buffers = new RowBufferQueue<>(numBuffers);
            for (int i = 0; i < numBuffers; i++) {
                buffers.put(BufferedAccesses.createBufferedAccessRow(schema));
            }
            queue = new ArrayBlockingQueue<>(numBuffers);
            order = new RowOrderList<>(queue, buffers, 0);
        }

        void accept(final long rowIndex, final ReadAccessRow rowData) throws InterruptedException {
            var buffer = buffers.take(rowIndex);
            buffer.setFrom(rowData);
            synchronized (order) {
                order.accept(rowIndex, buffer);
            }
        }

        void skip(final long rowIndex) throws InterruptedException {
            synchronized (order) {
                order.skip(rowIndex);
            }
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
