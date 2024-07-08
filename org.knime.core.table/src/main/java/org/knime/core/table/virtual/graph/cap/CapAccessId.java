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
package org.knime.core.table.virtual.graph.cap;

import org.knime.core.table.access.ReadAccess;

/**
 * Identifies a particular {@link ReadAccess} by the producer {@code CapNode} and
 * the slot index in the producer outputs.
 * <p>
 * Inputs and outputs may be sparsely populated with respect to column index. For
 * example, if certain columns of a source table are never used in a virtual table
 * construction, the output {@code CapAccessId}s of the {@code SOURCE} node will have
 * "holes" at the respective column indices.
 * <p>
 * The index of an access in the {@code CapAccessId} collection is called "slot
 * index". That is, the {@code CapAccessId} at slot index 0 is the smallest
 * "non-hole" column index.
 */
public final class CapAccessId {

    private final CapNode producer;

    private final int slot;

    private final CapNode validity;

    public CapAccessId(final CapNode producer, final int slot) {
        this(producer, slot, producer);
    }

    public CapAccessId(final CapNode producer, final int slot, final CapNode validity) {
        this.producer = producer;
        this.slot = slot;
        this.validity = validity;
    }

    public CapAccessId withValidity(final CapNode validity) {
        return new CapAccessId(producer, slot, validity);
    }

    public CapNode producer() {
        return producer;
    }

    public int slot() {
        return slot;
    }

    public CapNode validity() {
        return validity;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof CapAccessId other))
            return false;

        return slot == other.slot && producer.equals(other.producer);
    }

    @Override
    public int hashCode() {
        return 31 * producer.hashCode() + slot;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("(");
        sb.append(producer.index());
        sb.append("::").append(slot);
        if ( validity != null ) {
            sb.append("|").append(validity.index());
        }
        sb.append(')');
        return sb.toString();
    }
}
