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
 *
 * History
 *   Apr 14, 2021 (marcel): created
 */
package org.knime.core.table.virtual.spec;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

// TODO: permute and column filter could be merged. Both columnIndices there and permutation here just specify which
// columns of the original table will be contained in the new table. Figure out if we lose any optimization capabilities
// when merging them (e.g. maybe there are column-filter vs permute specific optimizations that cannot be applied to the
// generalized, merged transformation). If we don't merge them, then we should enforce some preconditions/define some
// contracts (e.g. in the case of permutation, the length of the input permutation array must be the same as the number
// of columns of the original table; in the case of column filtering it must be less or equal; etc.).
public final class PermuteTransformSpec implements TableTransformSpec {

    private final int[] m_permutation;

    public PermuteTransformSpec(final int[] permutation) {
        m_permutation = permutation.clone();
        final Set<Integer> set = new HashSet<>(m_permutation.length);
        for (final int columnIndex : permutation) {
            if (columnIndex < 0) {
                throw new IndexOutOfBoundsException(
                    "Permutation contains negative indices: " + Arrays.toString(permutation));
            }
            if (!set.add(columnIndex) || columnIndex >= m_permutation.length) {
                throw new IllegalArgumentException(
                    "Permutation contains duplicate indices or has holes: " + Arrays.toString(permutation));
            }
        }
    }

    /**
     * @return the permutation
     */
    public int[] getPermutation() {
        return m_permutation.clone();
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(m_permutation);
    }

    @Override
    public boolean equals(final Object obj) {
        return obj instanceof PermuteTransformSpec &&
            Arrays.equals(m_permutation, ((PermuteTransformSpec)obj).m_permutation);
    }

    @Override
    public String toString() {
        return "Permute " + Arrays.toString(m_permutation);
    }
}
