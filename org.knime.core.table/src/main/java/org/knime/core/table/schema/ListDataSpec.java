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
 *   19 Apr 2021 (Marc): created
 */
package org.knime.core.table.schema;

import org.knime.core.table.schema.traits.DataTraits;
import org.knime.core.table.schema.traits.ListDataTraits;

/**
 * The {@link DataSpec} for list data.
 */
public final class ListDataSpec implements DataSpec {

    private final DataSpec m_inner;

    /**
     * Create a spec for list data, in which lists can hold objects according to a given {@link DataSpec}.
     *
     * @param inner the spec for the elements the lists consist of
     */
    public ListDataSpec(final DataSpec inner) {
        m_inner = inner;
    }

    /**
     * @return the {@link DataSpec} of the elements the lists consist of
     */
    public DataSpec getInner() {
        return m_inner;
    }

    @Override
    public <R> R accept(final Mapper<R> v) {
        return v.visit(this);
    }

    @Override
    public <R> R accept(final MapperWithTraits<R> v, final DataTraits traits) {
        return v.visit(this, (ListDataTraits)traits);
    }

    @Override
    public int hashCode() {
        return 31 + m_inner.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        return obj instanceof ListDataSpec && m_inner.equals(((ListDataSpec)obj).m_inner);
    }

    @Override
    public String toString() {
        return "List (" + m_inner.toString() + ")";
    }

}