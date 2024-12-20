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
 */
package org.knime.core.table.virtual.graph.exec;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.virtual.graph.cap.CapAccessId;
import org.knime.core.table.virtual.graph.cap.CapNode;
import org.knime.core.table.virtual.graph.cap.CapNodeAppend;
import org.knime.core.table.virtual.graph.cap.CapNodeConcatenate;
import org.knime.core.table.virtual.graph.cap.CapNodeConsumer;
import org.knime.core.table.virtual.graph.cap.CapNodeMap;
import org.knime.core.table.virtual.graph.cap.CapNodeMissing;
import org.knime.core.table.virtual.graph.cap.CapNodeObserver;
import org.knime.core.table.virtual.graph.cap.CapNodeRowFilter;
import org.knime.core.table.virtual.graph.cap.CapNodeRowIndex;
import org.knime.core.table.virtual.graph.cap.CapNodeSlice;
import org.knime.core.table.virtual.graph.cap.CapNodeSource;

class AssembleNodeImps {

    private final List<SequentialNodeImp> imps;

    public AssembleNodeImps( //
            final List<CapNode> cap, //
            final List<RowAccessible> sources) {

        imps = new ArrayList<>(cap.size());
        final Iterator<RowAccessible> sourceIter = sources.iterator();
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
        if (imp instanceof SequentialNodeImpConsumer) {
            return (SequentialNodeImpConsumer)imp;
        }
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
