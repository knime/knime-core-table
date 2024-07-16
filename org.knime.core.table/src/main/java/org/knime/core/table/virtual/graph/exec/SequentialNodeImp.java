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

interface SequentialNodeImp extends NodeImp {

    /**
     * Recursively call {@link #forward} on predecessors, possibly multiple
     * times. For example, {@link SequentialNodeImpRowFilter row filter} repeatedly calls
     * {@code predecessor.forward()} until the filter predicate accepts the
     * current values (current row).
     *
     * @return {@code true} if forwarding was successful, {@code false}
     *         otherwise (i.e. at the end)
     */
    boolean forward();

    /*
     * Recursively call {@link #canForward} on predecessors.
     * <p>
     * Some {@code NodeImp}s simply throw an exception because they cannot
     * figure out whether it is possible to forward without actually doing so.
     * {@code #canForward} will be used in the created cursor only if the comp
     * graph figured out that it is possible.
     *
     * @return {@code true} if more elements are available, that is, the next
     *         {@link #forward} would return {@code true}.
     */
    boolean canForward();

    /**
     * Whether the {@link #getOutput output} {@code ReadAccess}es of this {@code
     * SequentialNodeImp} are currently valid. Equivalently: whether the
     * previous {@link #forward()} was successful.
     *
     * @return {@code true} if the outputs of this node are currently valid.
     */
    boolean isValid();
}
