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
package org.knime.core.table.virtual.graph.rag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.virtual.graph.rag.RagNode.AccessValidity;

/**
 * {@code AccessId} represents a particular {@link ReadAccess}, identified by a
 * producer {@link RagNode} and the (output) column index in that node. {@code
 * AccessId} also keeps track of all consumers ({@link RagNode}s that use the access).
 */
public final class AccessId {

    private final RagNode producer;

    // set of nodes consuming the access (no duplicates)
    private final List<RagNode> consumers = new ArrayList<>();

    private final List<RagNode> unmodifiableConsumers = Collections.unmodifiableList(consumers);

    private final int columnIndex;

    private final AccessValidity validity;

    public AccessId(final RagNode producer, final int columnIndex, final AccessValidity validity) {
        this.producer = producer;
        this.columnIndex = columnIndex;
        this.validity = validity;
        validity.addConsumer(this);
    }

    public RagNode getProducer() {
        return producer;
    }

    /**
     * Get the (output) column index of this access in the producer node.
     */
    public int getColumnIndex() {
        return columnIndex;
    }

    public List<RagNode> getConsumers() {
        return unmodifiableConsumers;
    }

    public void addConsumer(RagNode node) {
        if (!consumers.contains(node))
            consumers.add(node);
    }

    /**
     * For each {@link #getConsumers() consumer} of this {@code AccessId}:
     * replace {@code this} with {@code newId} in the inputs of {@code
     * consumer}, add {@code consumer} to the consumers of {@code newId}.
     * Finally, clear the consumers of this {@code AccessId}.
     *
     * @param newId
     * @return {@code true} if any replacement was made (that is, unless {@link #getConsumers()} is empty).
     */
    public boolean replaceInConsumersWith(final AccessId newId) {
        if (consumers.isEmpty())
            return false;

        for (RagNode consumer : consumers) {
            consumer.replaceInput(this, newId);
            newId.addConsumer(consumer);
        }
        consumers.clear();
        return true;
    }

    public void removeConsumer(RagNode node) {
        consumers.remove(node);
    }

//    @Override
//    public String toString() {
//        final StringBuilder sb = new StringBuilder("AccessId{");
//        sb.append("producer=<").append(producer.id).append(">");
//        sb.append(", columnIndex=").append(columnIndex);
//        sb.append('}');
//        return sb.toString();
//    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("<").append(producer.id()).append(">:");
        sb.append(columnIndex);
        sb.append('}');
        return sb.toString();
    }
}
