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

import java.util.Arrays;
import java.util.UUID;

/**
 * Represents a sink of a CAP.
 * <p>
 * A {@code CapNodeMaterialize} knows the {@code CapAccessId}s (producer-slot pairs)
 * of the {@code ReadAccess}es that should be materialized into the output {@code
 * RowWriteAccessible}, and the index of the predecessor {@code CapNode}.
 */
public class CapNodeMaterialize extends CapNode {

    private final UUID uuid;
    private final CapAccessId[] inputs;
    private final int predecessor;

    /**
     *
     * @param index index of this node in the CAP list.
     * @param uuid  the UUID of the output table
     * @param inputs input columns to be materialized
     * @param predecessor index of the predecessor node in the CAP list.
     */
    public CapNodeMaterialize(final int index, final UUID uuid, CapAccessId[] inputs, final int predecessor) {
        super(index, CapNodeType.MATERIALIZE);
        this.uuid = uuid;
        this.inputs = inputs;
        this.predecessor = predecessor;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MATERIALIZE(");
        sb.append("uuid=").append(uuid);
        sb.append(", predecessor=").append(predecessor);
        sb.append(", inputs=").append(Arrays.toString(inputs));
        sb.append(')');
        return sb.toString();
    }

    /**
     * @return the UUID of the output table represented by this {@code CapNodeMaterialize}.
     */
    public UUID uuid() {
        return uuid;
    }

    /**
     * @return the {@code CapAccessId}s (producer-slot pairs) of the {@code ReadAccess}es
     *         that should bematerialized into the output {@code RowWriteAccessible}
     */
    public CapAccessId[] inputs() {
        return inputs;
    }

    /**
     * A {@code CapNodeMaterialize} has exactly one predecessor. Calling {@code forward()} on
     * the (instantiation of the) consumer will call {@code forward()} on the (instantiation
     * of the) predecessor.
     *
     * @return the index of the predecessor node in the CAP list.
     */
    public int predecessor() {
        return predecessor;
    }
}
