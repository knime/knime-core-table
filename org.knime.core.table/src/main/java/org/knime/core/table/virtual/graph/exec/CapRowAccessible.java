package org.knime.core.table.virtual.graph.exec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.knime.core.table.cursor.Cursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.row.Selection;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.graph.cap.CapAccessId;
import org.knime.core.table.virtual.graph.cap.CapNode;
import org.knime.core.table.virtual.graph.cap.CapNodeAppend;
import org.knime.core.table.virtual.graph.cap.CapNodeConcatenate;
import org.knime.core.table.virtual.graph.cap.CapNodeConsumer;
import org.knime.core.table.virtual.graph.cap.CapNodeMap;
import org.knime.core.table.virtual.graph.cap.CapNodeMissing;
import org.knime.core.table.virtual.graph.cap.CapNodeRowFilter;
import org.knime.core.table.virtual.graph.cap.CapNodeSlice;
import org.knime.core.table.virtual.graph.cap.CapNodeSource;
import org.knime.core.table.virtual.graph.cap.CursorAssemblyPlan;

class CapRowAccessible implements RowAccessible {

    private final ColumnarSchema schema;

    private final CursorAssemblyPlan cap;

    private final List<RowAccessible> sources;

    CapRowAccessible(final ColumnarSchema schema, final CursorAssemblyPlan cap, final List<RowAccessible> sources) {
        this.schema = schema;
        this.cap = cap;
        this.sources = sources;
    }

    @Override
    public ColumnarSchema getSchema() {
        return schema;
    }

    @Override
    public Cursor<ReadAccessRow> createCursor() {
        return new CapCursor(assembleConsumer());
    }

    @Override
    public Cursor<ReadAccessRow> createCursor(final Selection selection) {
        // TODO: What should happen here? The CAP already has column/row selection baked
        //   in. We would have to create a new comp graph. This should probably not be done
        //   here, though...
        if (selection.allSelected()) {
            return createCursor();
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public long size() {
        return cap.numRows();
    }

    @Override
    public void close() throws IOException {
        // TODO ?
    }

    NodeImpConsumer assembleConsumer() {
        return new AssembleConsumer().get(cap.nodes(), sources);
    }

    static class AssembleConsumer {

        private List<NodeImp> imps;

        private NodeImpConsumer get(final List<CapNode> cap, final List<RowAccessible> sources) {
            imps = new ArrayList<>(cap.size());
            final Iterator<RowAccessible> sourceIter = sources.iterator();
            for (CapNode node : cap) {
                switch (node.type()) {
                    case SOURCE: {
                        final CapNodeSource source = (CapNodeSource)node;
                        imps.add(new NodeImpSource(sourceIter.next(), source.cols(), source.fromRow(), source.toRow()));
                        break;
                    }
                    case MISSING: {
                        final CapNodeMissing source = (CapNodeMissing)node;
                        imps.add(new NodeImpMissing(source.missingValueSpecs()));
                        break;
                    }
                    case SLICE: {
                        final CapNodeSlice slice = (CapNodeSlice)node;
                        imps.add(new NodeImpSlice(imps.get(slice.predecessor()), slice.from(), slice.to()));
                        break;
                    }
                    case ROWFILTER: {
                        final CapNodeRowFilter rowfilter = (CapNodeRowFilter)node;
                        final AccessImp[] inputs = accessImps(rowfilter.inputs());
                        imps.add(new NodeImpRowFilter(inputs, imps.get(rowfilter.predecessor()), rowfilter.filterFactory()));
                        break;
                    }
                    case MAP: {
                        final CapNodeMap map = (CapNodeMap)node;
                        final AccessImp[] inputs = accessImps(map.inputs());
                        imps.add(new NodeImpMap(inputs, imps.get(map.predecessor()), map.mapOutputSpecs(), map.cols(),
                                map.mapperFactory()));
                        break;
                    }
                    case APPEND: {
                        final CapNodeAppend append = (CapNodeAppend)node;
                        final AccessImp[] inputs = accessImps(append.inputs());
                        final NodeImp[] predecessors = nodeImps(append.predecessors());
                        imps.add(new NodeImpAppend(inputs, predecessors, append.predecessorOutputIndices()));
                        break;
                    }
                    case CONCATENATE: {
                        final CapNodeConcatenate concatenate = (CapNodeConcatenate)node;
                        final CapAccessId[][] capInputs = concatenate.inputs();
                        final AccessImp[][] inputs = new AccessImp[capInputs.length][];
                        Arrays.setAll(inputs, i -> accessImps(capInputs[i]));
                        final NodeImp[] predecessors = nodeImps(concatenate.predecessors());
                        imps.add(new NodeImpConcatenate(inputs, predecessors));
                        break;
                    }
                    case CONSUMER: {
                        final CapNodeConsumer consumer = (CapNodeConsumer)node;
                        final AccessImp[] inputs = accessImps(consumer.inputs());
                        return new NodeImpConsumer(inputs, imps.get(consumer.predecessor()));
                    }
                }
            }
            throw new IllegalArgumentException("CAP doesn't contain a CONSUMER");
        }

        private AccessImp[] accessImps(final CapAccessId[] capAccessIds) {
            final AccessImp[] accessImps = new AccessImp[capAccessIds.length];
            Arrays.setAll(accessImps, i -> {
                final CapAccessId a = capAccessIds[i];
                return new AccessImp(imps.get(a.producer().index()), a.slot());
            });
            return accessImps;
        }

        private NodeImp[] nodeImps(final int[] capNodeIndices) {
            final NodeImp[] nodeImps = new NodeImp[capNodeIndices.length];
            Arrays.setAll(nodeImps, i -> imps.get(capNodeIndices[i]));
            return nodeImps;
        }
    }
}
