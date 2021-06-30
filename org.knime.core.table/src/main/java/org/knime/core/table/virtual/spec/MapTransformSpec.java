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
 *   May 4, 2021 (marcel): created
 */
package org.knime.core.table.virtual.spec;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
public final class MapTransformSpec implements TableTransformSpec {

    private final MapperSpec m_factory;

    // TODO: the mappers expected here are probably the most generic ones that can accomplish a row-wise map of a table.
    // They, however, do not make any guarantees as to which cells of the input row are replaced/left untouched/etc. to
    // produce the output row. Some more specialized/limited kind of map (e.g. one that only replaces a single cell per
    // row, of the same column across all rows) would allow for more optimization (because we know that all other cells
    // were not touched in the example).
    // TODO: We provide a spec to create potentially many different instances of the same mapper (e.g. one per thread) -
    // meaning we're not requiring a mapper to be stateless.
    // TODO: Special MapperSpec could be "UnaryDoubleMapper" or "BinaryDoubleMapper" or "Expression" which are then
    // wrapped into a MapperSpec for Row-wise operations. However, Arrow could then unpack these mappers and apply them
    // directly on the vectors.
    public MapTransformSpec(final MapperSpec spec) {
        m_factory = spec;
    }

    /**
     * @return the factory
     */
    public MapperSpec getFactory() {
        return m_factory;
    }

    // TODO: mappers are generally binary black boxes. However, for some special, declarative mappers (i.e.
    // expressions; see above), it could make sense to override hashCode, equals, and toString here. Also, such mappers
    // would be serializable.
}
