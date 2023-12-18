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
