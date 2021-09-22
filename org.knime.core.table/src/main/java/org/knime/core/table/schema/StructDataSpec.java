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

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.knime.core.table.schema.traits.DataTraits;
import org.knime.core.table.schema.traits.StructDataTraits;

/**
 * The {@link DataSpec} for struct data.
 */
public final class StructDataSpec implements DataSpec {

    private final DataSpec[] m_inner;

    /**
     * Create a spec for struct data, in which structs hold objects according to a given array of
     * {@link DataSpec ColumnDataSpecs}.
     *
     * @param inner the specs for the elements the structs consist of
     */
    public StructDataSpec(final DataSpec... inner) {
        m_inner = inner;
    }

    /**
     * Get the {@link DataSpec} of the {@code i}-th struct element.
     *
     * @param i index of the struct element
     * @return spec of the {@code i}-th struct element
     */
    public DataSpec getDataSpec(final int i) {
        return m_inner[i];
    }

    /**
     * @return number of elements of this struct
     */
    public int size() {
        return m_inner.length;
    }

    @Override
    public <R> R accept(final Mapper<R> v) {
        return v.visit(this);
    }

    @Override
    public <R> R accept(final MapperWithTraits<R> v, final DataTraits traits) {
        return v.visit(this, (StructDataTraits)traits);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(m_inner);
    }

    @Override
    public boolean equals(final Object obj) {
        return obj instanceof StructDataSpec && Arrays.equals(m_inner, ((StructDataSpec)obj).m_inner);
    }

    @Override
    public String toString() {
        return Stream.of(m_inner).map(DataSpec::toString).collect(Collectors.joining(", ", "Struct {", "}"));
    }

}