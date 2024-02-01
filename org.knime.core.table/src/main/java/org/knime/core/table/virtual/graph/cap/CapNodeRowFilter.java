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

import org.knime.core.table.virtual.spec.RowFilterTransformSpec;
import org.knime.core.table.virtual.spec.RowFilterTransformSpec.RowFilterFactory;

/**
 * Represents a row-filter operation in the CAP.
 * <p>
 * A {@code CapNodeRowFilter} knows the {@code CapAccessId}s (producer-slot pairs)
 * of the {@code ReadAccess}es required by the filter operation, the filter
 * operation, and the index of the predecessor {@code CapNode}.
 */
public class CapNodeRowFilter extends CapNode {

    private final CapAccessId[] inputs;
    private final int predecessor;
    private final RowFilterFactory filterFactory;

    public CapNodeRowFilter(final int index, final CapAccessId[] inputs, final int predecessor,
            RowFilterFactory filterFactory) {
        super(index, CapNodeType.ROWFILTER);
        this.inputs = inputs;
        this.predecessor = predecessor;
        this.filterFactory = filterFactory;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ROWFILTER(");
        sb.append("inputs=").append(Arrays.toString(inputs));
        sb.append(", predecessor=").append(predecessor);
        sb.append(", filterFactory=").append(filterFactory);
        sb.append(')');
        return sb.toString();
    }

    /**
     * @return the {@code CapAccessId}s (producer-slot pairs) of the {@code ReadAccess}es required by the filter operation
     */
    public CapAccessId[] inputs() {
        return inputs;
    }

    /**
     * A {@code CapNodeRowFilter} has exactly one predecessor. Calling {@code
     * forward()} on the (instantiation of the) row-filter will repeatedly call {@code
     * forward()} on the (instantiation of the) predecessor and evaluate the filter
     * predicate, until the filter predicated passes or the predecessor is exhausted.
     *
     * @return the index of the predecessor node in the CAP list.
     */
    public int predecessor() {
        return predecessor;
    }

    /**
     * @return the filter factory
     */
    public RowFilterFactory filterFactory() {
        return filterFactory;
    }
}
