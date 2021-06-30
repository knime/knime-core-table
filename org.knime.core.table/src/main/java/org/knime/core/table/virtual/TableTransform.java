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
 *   Apr 12, 2021 (marcel): created
 */
package org.knime.core.table.virtual;

import java.util.Collections;
import java.util.List;

import org.knime.core.table.virtual.spec.SourceTransformSpec;
import org.knime.core.table.virtual.spec.TableTransformSpec;

// TODO split into source, node, sink?
public final class TableTransform {

    private final List<TableTransform> m_precedingTransforms;

    private final TableTransformSpec m_spec;

    public TableTransform(final SourceTransformSpec spec) {
        m_precedingTransforms = Collections.emptyList();
        m_spec = spec;
    }

    public TableTransform(final List<TableTransform> precedingTransforms, final TableTransformSpec spec) {
        m_precedingTransforms = Collections.unmodifiableList(precedingTransforms);
        m_spec = spec;
    }

    /**
     * @return The specification of this transformation. Can be a {@link SourceTransformSpec}, in which case
     *         {@link #getPrecedingTransforms()} returns an empty list.
     */
    public TableTransformSpec getSpec() {
        return m_spec;
    }

    /**
     * @return Empty if {@link #getSpec()} returns a {@link SourceTransformSpec}.
     */
    public List<TableTransform> getPrecedingTransforms() {
        return m_precedingTransforms;
    }

    @Override
    public int hashCode() {
        return m_spec.hashCode() * 31 + m_precedingTransforms.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof TableTransform)) {
            return false;
        }
        final TableTransform other = (TableTransform)obj;
        return m_spec.equals(other.m_spec) && m_precedingTransforms.equals(other.m_precedingTransforms);
    }

    @Override
    public String toString() {
        return "Transform: " + m_spec.toString() + "; " + m_precedingTransforms.size() + " predecessor(s)";
    }
}
