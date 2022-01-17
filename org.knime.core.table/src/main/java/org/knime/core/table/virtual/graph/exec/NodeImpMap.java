package org.knime.core.table.virtual.graph.exec;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.knime.core.table.access.BufferedAccesses;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.virtual.spec.MapTransformSpec;

class NodeImpMap implements NodeImp {
    private final AccessImp[] inputs;

    private final ReadAccess[] mapInputs;

    private final BufferedAccesses.BufferedAccess[] mapOutputs;

    private final MapTransformSpec.Map map;

    private final ReadAccess[] outputs;

    private final NodeImp predecessor;

    /**
     * @param mapOutputSpecs these accesses are needed as outputs for the {@code map()} function.
     * @param cols           these indices among {@code mapOutputSpecs} are the outputs of this NodeImp
     * @param map
     */
    public NodeImpMap(final AccessImp[] inputs, final NodeImp predecessor, final List<DataSpec> mapOutputSpecs,
            final int[] cols, MapTransformSpec.Map map) {
        this.inputs = inputs;
        this.predecessor = predecessor;

        mapInputs = new ReadAccess[inputs.length];
        mapOutputs = new BufferedAccesses.BufferedAccess[mapOutputSpecs.size()];
        this.map = map;
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
    }

    @Override
    public void create() {
        predecessor.create();
        link();
    }

    @Override
    public boolean forward() {
        final boolean result = predecessor.forward();
        map.map(mapInputs, mapOutputs);
        return result;
    }

    @Override
    public void close() throws IOException {
        predecessor.close();
    }
}
