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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.knime.core.table.access.BufferedAccesses;
import org.knime.core.table.access.BufferedAccesses.BufferedAccess;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.access.WriteAccess;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.virtual.spec.MapTransformSpec.MapperFactory;

class SequentialNodeImpMap implements SequentialNodeImp {
    private final AccessImp[] inputs;

    private final ReadAccess[] mapInputs;

    private final BufferedAccess[] mapOutputs;

    private final MapperFactory mapperFactory;

    private Runnable mapper;

    private final ReadAccess[] outputs;

    private final SequentialNodeImp predecessor;

    /**
     * @param mapOutputSpecs these accesses are needed as outputs for the {@code map()} function.
     * @param cols           these indices among {@code mapOutputSpecs} are the outputs of this NodeImp
     * @param mapperFactory
     */
    SequentialNodeImpMap(final AccessImp[] inputs, final SequentialNodeImp predecessor, final List<DataSpec> mapOutputSpecs,
            final int[] cols, final MapperFactory mapperFactory) {
        this.inputs = inputs;
        this.predecessor = predecessor;

        mapInputs = new ReadAccess[inputs.length];
        mapOutputs = new BufferedAccess[mapOutputSpecs.size()];
        this.mapperFactory = mapperFactory;
        Arrays.setAll(mapOutputs, i -> BufferedAccesses.createBufferedAccess(mapOutputSpecs.get(i)));

        outputs = new ReadAccess[cols.length];
        Arrays.setAll(outputs, i -> mapOutputs[cols[i]]);
    }

    @Override
    public ReadAccess getOutput(final int i) {
        return outputs[i];
    }

    @Override
    public boolean isValid() {
        // should never be called
        throw new UnsupportedOperationException();
    }

    private void link() {
        for (int i = 0; i < inputs.length; i++) {
            mapInputs[i] = inputs[i].getReadAccess();
        }
        mapper = mapperFactory.createMapper(mapInputs, mapOutputs);
    }

    @Override
    public void create() {
        predecessor.create();
        link();
    }

    @Override
    public boolean forward() {
        if (predecessor.forward()) {
            // As per buffered access contract, we need to set all fields to missing if we're writing to a new row.
            // We don't know whether the user provided mapper will write a value to each cell, so we call setMissing.
            Arrays.stream(mapOutputs).forEach(WriteAccess::setMissing);
            mapper.run();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean canForward() {
        return predecessor.canForward();
    }

    @Override
    public void close() throws IOException {
        predecessor.close();
    }
}
