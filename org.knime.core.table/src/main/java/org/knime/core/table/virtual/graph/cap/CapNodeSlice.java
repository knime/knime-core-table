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

/**
 * Represents a slice operation in the CAP.
 * <p>
 * A {@code CapNodeSlice} knows the row index range {@code [from, to)}, and the
 * index of the predecessor {@code CapNode}.
 */
public class CapNodeSlice extends CapNode {

    private final int predecessor;
    private final long from;
    private final long to;

    /**
     * @param index index of this node in the CAP list.
     * @param predecessor index of the predecessor node in the CAP list.
     * @param from start (row) index of the slice (inclusive).
     * @param to end (row) index of the slice (exclusive).
     */
    public CapNodeSlice(final int index, final int predecessor, final long from, final long to) {
        super(index, CapNodeType.SLICE);
        this.predecessor = predecessor;
        this.from = from;
        this.to = to;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SLICE(");
        sb.append("predecessor=").append(predecessor);
        sb.append(", from=").append(from);
        sb.append(", to=").append(to);
        sb.append(')');
        return sb.toString();
    }

    /**
     * A {@code CapNodeSlice} has exactly one predecessor. Calling {@code forward()} on
     * the (instantiation of the) Slice will call {@code forward()} on the
     * (instantiation of the) predecessor: The first {@code forward()} advances the
     * predecessor {@link #from()} times. Each subsequent {@code forward()} advances
     * the predecessor once, until predecessor has been advanced {@link #to()}-1 times
     * in total.
     *
     * @return the index of the predecessor node in the CAP list.
     */
    public int predecessor() {
        return predecessor;
    }

    /**
     * @return start (row) index of the slice (inclusive).
     */
    public long from() {
        return from;
    }

    /**
     * @return end (row) index of the slice (exclusive).
     */
    public long to() {
        return to;
    }
}
