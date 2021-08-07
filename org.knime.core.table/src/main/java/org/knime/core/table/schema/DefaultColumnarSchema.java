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
package org.knime.core.table.schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.knime.core.table.schema.traits.DataTraits;

import com.google.common.collect.Iterators;

public final class DefaultColumnarSchema implements ColumnarSchema {

    private final List<DataSpec> m_columnSpecs;

    private final List<DataTraits> m_columnTraits;

    public DefaultColumnarSchema(final DataSpec columnSpec, final DataTraits columnTraits) {
        this(Arrays.asList(columnSpec), Arrays.asList(columnTraits));
    }

    public DefaultColumnarSchema(final DataSpec[] columnSpecs, final DataTraits[] columnTraits) {
        this(Arrays.asList(columnSpecs), Arrays.asList(columnTraits));
    }

    public DefaultColumnarSchema(final List<DataSpec> columnSpecs, final List<DataTraits> columnTraits) {
        m_columnSpecs = Collections.unmodifiableList(new ArrayList<>(columnSpecs));
        m_columnTraits = Collections.unmodifiableList(new ArrayList<>(columnTraits));
    }

    @Override
    public int numColumns() {
        return m_columnSpecs.size();
    }

    @Override
    public DataSpec getSpec(final int index) {
        return m_columnSpecs.get(index);
    }

    @Override
    public DataTraits getTraits(final int index) {
        return m_columnTraits.get(index);
    }

    @Override
    public Iterator<DataSpec> iterator() {
        return m_columnSpecs.iterator();
    }

    @Override
    public int hashCode() {
        return m_columnSpecs.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof ColumnarSchema)) { // NOSONAR
            return false;
        }
        if (obj instanceof DefaultColumnarSchema) {
            return m_columnSpecs.equals(((DefaultColumnarSchema)obj).m_columnSpecs);
        }
        final ColumnarSchema other = (ColumnarSchema)obj;
        if (numColumns() != other.numColumns()) {
            return false;
        }
        return Iterators.elementsEqual(iterator(), other.iterator());
    }

    @Override
    public String toString() {
        return "Columns (" + m_columnSpecs.size() + ") " + m_columnSpecs;
    }
}
