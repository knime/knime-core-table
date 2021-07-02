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
package org.knime.core.table.schema;

import org.knime.core.table.schema.traits.DataTraits;

/**
 * The columnar schema of a table.
 * <p>
 * Implementations of this interface must implement {@link #equals(Object)} and
 * {@link #hashCode()} as specified to ensure comparability across implementations.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public interface ColumnarSchema extends Iterable<DataSpec> {

    /**
     * Obtain the number of columns in the table described by this schema.
     *
     * @return the number of columns in the store
     */
    int numColumns();

    /**
     * Obtain the {@link DataSpec} of the column at a given index.
     *
     * @param index the index of the column for which to obtain the spec
     * @return the column's spec
     * @throws IndexOutOfBoundsException if the index is negative or equal to or greater than the number of columns
     */
    DataSpec getSpec(int index);

    /**
     * Return the {@link DataTraits} of the selected column
     *
     * @param index the index of the column for which to query the traits
     * @return the {@link DataTraits}
     * @throws IndexOutOfBoundsException if the index is negative or equal to or greater than the number of columns
     */
    DataTraits getTraits(int index);

    /**
     * Compares the specified object with this {@code ColumnarSchema} for
     * equality. Returns {@code true} if and only if the specified object is
     * also a {@code ColumnarSchema}, and contains the same {@code DataSpec}s
     * (as determined by {@code Objects.equals(e1, e2)}, in the same order.
     */
    @Override boolean equals(Object o);

    /**
     * Returns the hash code value for this {@code ColumnarSchema}. The hash code
     * is defined to be the result of the following calculation:
     * <pre>{@code
     *     int hashCode = 1;
     *     for (E e : list)
     *         hashCode = 31*hashCode + (e==null ? 0 : e.hashCode());
     * }</pre>
     */
    @Override int hashCode();
}
