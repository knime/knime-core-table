package org.knime.core.table.virtual.graph.exec;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.knime.core.table.access.BufferedAccesses;
import org.knime.core.table.access.BufferedAccesses.BufferedAccess;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.virtual.spec.MapTransformSpec.MapperFactory;

class NodeImpMap implements NodeImp {
    private final AccessImp[] inputs;

    private final ReadAccess[] mapInputs;

    private final BufferedAccess[] mapOutputs;

    private final MapperFactory mapperFactory;

    private Runnable mapper;

    private final ReadAccess[] outputs;

    private final NodeImp predecessor;

    /**
     * @param mapOutputSpecs these accesses are needed as outputs for the {@code map()} function.
     * @param cols           these indices among {@code mapOutputSpecs} are the outputs of this NodeImp
     * @param mapperFactory
     */
    public NodeImpMap(final AccessImp[] inputs, final NodeImp predecessor, final List<DataSpec> mapOutputSpecs,
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
    public ReadAccess getOutput(int i) {
        return outputs[i];
    }

    private void link() {
        for (int i = 0; i < inputs.length; i++) {
            AccessImp input = inputs[i];
            mapInputs[i] = input.node.getOutput(input.i);
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
            mapper.run();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void close() throws IOException {
        predecessor.close();
    }
}
