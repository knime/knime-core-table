package org.knime.core.table.virtual.graph.exec;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.row.RowWriteAccessible;
import org.knime.core.table.virtual.graph.cap.CapAccessId;
import org.knime.core.table.virtual.graph.cap.CapNode;
import org.knime.core.table.virtual.graph.cap.CapNodeAppend;
import org.knime.core.table.virtual.graph.cap.CapNodeConcatenate;
import org.knime.core.table.virtual.graph.cap.CapNodeConsumer;
import org.knime.core.table.virtual.graph.cap.CapNodeMap;
import org.knime.core.table.virtual.graph.cap.CapNodeMaterialize;
import org.knime.core.table.virtual.graph.cap.CapNodeMissing;
import org.knime.core.table.virtual.graph.cap.CapNodeObserver;
import org.knime.core.table.virtual.graph.cap.CapNodeRowFilter;
import org.knime.core.table.virtual.graph.cap.CapNodeRowIndex;
import org.knime.core.table.virtual.graph.cap.CapNodeSlice;
import org.knime.core.table.virtual.graph.cap.CapNodeSource;

class AssembleNodeImps {

    private final List<SequentialNodeImp> imps;

    public AssembleNodeImps(
            final List<CapNode> cap,
            final List<RowAccessible> sources) {

        this(cap, sources, Collections.emptyList());
    }

    public AssembleNodeImps( //
            final List<CapNode> cap, //
            final List<RowAccessible> sources, //
            final List<RowWriteAccessible> sinks) {

        imps = new ArrayList<>(cap.size());
        final Iterator<RowAccessible> sourceIter = sources.iterator();
        final Iterator<RowWriteAccessible> sinksIter = sinks.iterator();
        for (CapNode node : cap) {
            switch (node.type()) {
                case SOURCE: {
                    final CapNodeSource source = (CapNodeSource)node;
                    imps.add(new SequentialNodeImpSource(sourceIter.next(), source.cols(), source.fromRow(), source.toRow()));
                    break;
                }
                case MISSING: {
                    final CapNodeMissing source = (CapNodeMissing)node;
                    imps.add(new SequentialNodeImpMissing(source.missingValueSpecs()));
                    break;
                }
                case SLICE: {
                    final CapNodeSlice slice = (CapNodeSlice)node;
                    imps.add(new SequentialNodeImpSlice(imps.get(slice.predecessor()), slice.from(), slice.to()));
                    break;
                }
                case ROWFILTER: {
                    final CapNodeRowFilter rowfilter = (CapNodeRowFilter)node;
                    final AccessImp[] inputs = accessImps(rowfilter.inputs());
                    imps.add(
                            new SequentialNodeImpRowFilter(inputs, imps.get(rowfilter.predecessor()), rowfilter.filterFactory()));
                    break;
                }
                case MAP: {
                    final CapNodeMap map = (CapNodeMap)node;
                    final AccessImp[] inputs = accessImps(map.inputs());
                    imps.add(new SequentialNodeImpMap(inputs, imps.get(map.predecessor()), map.mapOutputSpecs(), map.cols(),
                            map.mapperFactory()));
                    break;
                }
                case OBSERVER: {
                    final CapNodeObserver observer = (CapNodeObserver)node;
                    final AccessImp[] inputs = accessImps(observer.inputs());
                    imps.add(new SequentialNodeImpObserver(inputs, imps.get(observer.predecessor()), observer.observerFactory()));
                    break;
                }
                case ROWINDEX: {
                    final CapNodeRowIndex rowIndex = (CapNodeRowIndex)node;
                    imps.add(new SequentialNodeImpRowIndex(imps.get(rowIndex.predecessor()), rowIndex.offset()));
                    break;

                }
                case APPEND: {
                    final CapNodeAppend append = (CapNodeAppend)node;
                    final AccessImp[] inputs = accessImps(append.inputs());
                    final SequentialNodeImp[] predecessors = nodeImps(append.predecessors());
                    imps.add(new SequentialNodeImpAppend(inputs, predecessors, append.predecessorOutputIndices()));
                    break;
                }
                case CONCATENATE: {
                    final CapNodeConcatenate concatenate = (CapNodeConcatenate)node;
                    final CapAccessId[][] capInputs = concatenate.inputs();
                    final AccessImp[][] inputs = new AccessImp[capInputs.length][];
                    Arrays.setAll(inputs, i -> accessImps(capInputs[i]));
                    final SequentialNodeImp[] predecessors = nodeImps(concatenate.predecessors());
                    imps.add(new SequentialNodeImpConcatenate(inputs, predecessors));
                    break;
                }
                case CONSUMER: {
                    final CapNodeConsumer consumer = (CapNodeConsumer)node;
                    final AccessImp[] inputs = accessImps(consumer.inputs());
                    final SequentialNodeImp predecessor = imps.get(consumer.predecessor());
                    imps.add(new SequentialNodeImpConsumer(inputs, predecessor));
                    break;
                }
                case MATERIALIZE: {
                    final CapNodeMaterialize materialize = (CapNodeMaterialize)node;
                    final AccessImp[] inputs = accessImps(materialize.inputs());
                    final SequentialNodeImp predecessor = imps.get(materialize.predecessor());
                    imps.add(new SequentialNodeImpMaterialize(sinksIter.next(), inputs, predecessor));
                    break;
                }
                default:
                    throw new IllegalStateException("Unexpected value: " + node.type());
            }
        }
    }

    public SequentialNodeImp getTerminator()
    {
        return imps.get(imps.size() - 1);
    }

    public SequentialNodeImpConsumer getConsumer()
    {
        SequentialNodeImp imp = getTerminator();
        if (imp instanceof SequentialNodeImpConsumer)
            return (SequentialNodeImpConsumer)imp;
        throw new IllegalArgumentException("CAP doesn't end with CONSUMER");
    }

    private AccessImp[] accessImps(final CapAccessId[] capAccessIds) {
        final AccessImp[] accessImps = new AccessImp[capAccessIds.length];
        Arrays.setAll(accessImps, i -> {
            final CapAccessId a = capAccessIds[i];
            return new AccessImp(imps.get(a.producer().index()), a.slot());
        });
        return accessImps;
    }

    private SequentialNodeImp[] nodeImps(final int[] capNodeIndices) {
        final SequentialNodeImp[] nodeImps = new SequentialNodeImp[capNodeIndices.length];
        Arrays.setAll(nodeImps, i -> imps.get(capNodeIndices[i]));
        return nodeImps;
    }
}
