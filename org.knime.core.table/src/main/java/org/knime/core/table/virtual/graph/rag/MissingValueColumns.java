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
package org.knime.core.table.virtual.graph.rag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.DataSpecs.DataSpecWithTraits;
import org.knime.core.table.schema.traits.DataTraits;

class MissingValueColumns {

    private final List<DataSpecWithTraits> specs = new ArrayList<>();

    final List<DataSpecWithTraits> unmodifiable = Collections.unmodifiableList(specs);

    /**
     * We keep track of which types of missing-value columns we need. For example, we only need one
     * {@code DoubleDataSpec} column, even if there are multiple missing {@code DoubleDataSpec} columns used throughout
     * a {@code VirtualTable} construction.
     * <p>
     * Newly occurring {@code DataSpec}s are assigned consecutive {@code int} indices. These indices correspond to
     * column indices in the (singleton) MISSING node {@link RagGraph#getMissingValuesSource()}.
     *
     * @param spec a {@code DataSpec} for which a missing-value column is needed.
     * @return column index in {@link RagGraph#getMissingValuesSource() missing-values source}
     */
    public int getOrAdd(final DataSpec spec, final DataTraits traits) {
        var column = new DataSpecWithTraits(spec, traits);
        int i = specs.indexOf(column);
        if (i < 0) {
            i = specs.size();
            specs.add(column);
        }
        return i;
    }

}
