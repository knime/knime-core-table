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
import java.util.List;
import java.util.stream.Collectors;

import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.virtual.spec.MapTransformSpec.MapperFactory;

/**
 * Represents a map operation in the CAP.
 * <p>
 * A {@code CapNodeMap} knows the {@code CapAccessId}s (producer-slot pairs) of the
 * {@code ReadAccess}es required by the map function, the map function, the
 * {@code DataSpecs} of the map outputs, the column selection of the columns
 * produced by the map function, and the index of the predecessor {@code CapNode}.
 */
public class CapNodeMap extends CapNode {

    private final CapAccessId[] inputs;
    private final int predecessor;
    private final List<DataSpec> mapOutputSpecs;
    private final int[] cols;
    private final MapperFactory mapperFactory;

    public CapNodeMap(final int index, final CapAccessId[] inputs, final int predecessor,
            final int[] cols, final MapperFactory mapperFactory) {
        super(index, CapNodeType.MAP);
        this.inputs = inputs;
        this.predecessor = predecessor;
        this.mapOutputSpecs = mapperFactory.getOutputSchema().specStream().collect(Collectors.toList());
        this.cols = cols;
        this.mapperFactory = mapperFactory;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MAP(");
        sb.append("inputs=").append(Arrays.toString(inputs));
        sb.append(", predecessor=").append(predecessor);
        sb.append(", mapOutputSpecs=").append(mapOutputSpecs);
        sb.append(", cols=").append(Arrays.toString(cols));
        sb.append(", mapperFactory=").append(mapperFactory);
        sb.append(')');
        return sb.toString();
    }

    /**
     * @return the {@code CapAccessId}s (producer-slot pairs) of the {@code ReadAccess}es required by the map function
     */
    public CapAccessId[] inputs() {
        return inputs;
    }

    /**
     * A {@code CapNodeMap} has exactly one predecessor. Calling {@code forward()} on
     * the (instantiation of the) map will call {@code forward()} on the (instantiation
     * of the) predecessor and evaluate the map function.
     *
     * @return the index of the predecessor node in the CAP list.
     */
    public int predecessor() {
        return predecessor;
    }

    /**
     * @return DataSpecs of the columns produced by the map function
     */
    public List<DataSpec> mapOutputSpecs() {
        return mapOutputSpecs;
    }

    /**
     * A map function might produce multiple outputs, some of which might not be
     * consumed downstream. {@code cols()} selects among the columns produced by
     * the map function those that are required downstream. {@code cols()[i]} is the
     * index of a column produced by the map function. {@code i} is the output slot
     * index of this node which holds the column.
     *
     * @return the column selection of the map outputs
     */
    public int[] cols() {
        return cols;
    }

    /**
     * @return the mapper factory
     */
    public MapperFactory mapperFactory() {
        return mapperFactory;
    }
}
