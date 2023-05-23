package org.knime.core.table.virtual.graph.exec;

import java.util.ArrayList;
import java.util.Arrays;
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
import org.knime.core.table.virtual.graph.cap.CapNodeRowFilter;
import org.knime.core.table.virtual.graph.cap.CapNodeRowIndex;
import org.knime.core.table.virtual.graph.cap.CapNodeSlice;
import org.knime.core.table.virtual.graph.cap.CapNodeSource;

class AssembleNodeImps {

    private final List<NodeImp> imps;

    public AssembleNodeImps(
            final List<CapNode> cap,
            final List<RowAccessible> sources,
            final List<RowWriteAccessible> sinks) {
        imps = new ArrayList<>(cap.size());
        final Iterator<RowAccessible> sourceIter = sources.iterator();
        final Iterator<RowWriteAccessible> sinksIter = sinks.iterator();
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
                    imps.add(
                            new NodeImpRowFilter(inputs, imps.get(rowfilter.predecessor()), rowfilter.filterFactory()));
                    break;
                }
                case MAP: {
                    final CapNodeMap map = (CapNodeMap)node;
                    final AccessImp[] inputs = accessImps(map.inputs());
                    imps.add(new NodeImpMap(inputs, imps.get(map.predecessor()), map.mapOutputSpecs(), map.cols(),
                            map.mapperFactory()));
                    break;
                }
                case ROWINDEX: {
                    final CapNodeRowIndex rowIndex = (CapNodeRowIndex)node;
                    imps.add(new NodeImpRowIndex(imps.get(rowIndex.predecessor())));
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
                    final NodeImp predecessor = imps.get(consumer.predecessor());
                    imps.add(new NodeImpConsumer(inputs, predecessor));
                    break;
                }
                case MATERIALIZE: {
                    final CapNodeMaterialize materialize = (CapNodeMaterialize)node;
                    final AccessImp[] inputs = accessImps(materialize.inputs());
                    final NodeImp predecessor = imps.get(materialize.predecessor());
                    imps.add(new NodeImpMaterialize(sinksIter.next(), inputs, predecessor));
                    break;
                }
                default:
                    throw new IllegalStateException("Unexpected value: " + node.type());
            }
        }
    }

    public NodeImp getTerminator()
    {
        return imps.get(imps.size() - 1);
    }

    public NodeImpConsumer getConsumer()
    {
        NodeImp imp = getTerminator();
        if (imp instanceof NodeImpConsumer)
            return (NodeImpConsumer)imp;
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

    private NodeImp[] nodeImps(final int[] capNodeIndices) {
        final NodeImp[] nodeImps = new NodeImp[capNodeIndices.length];
        Arrays.setAll(nodeImps, i -> imps.get(capNodeIndices[i]));
        return nodeImps;
    }
}
