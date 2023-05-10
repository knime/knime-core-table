package org.knime.core.table.virtual.graph.exec;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.knime.core.table.access.BufferedAccesses;
import org.knime.core.table.access.BufferedAccesses.BufferedAccess;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.virtual.spec.MapTransformSpec.MapperFactory;

class RandomAccessNodeImpMap implements RandomAccessNodeImp {
    private final AccessImp[] inputs;

    private final ReadAccess[] mapInputs;

    private final BufferedAccess[] mapOutputs;

    private final MapperFactory mapperFactory;

    private Runnable mapper;

    private final ReadAccess[] outputs;

    private final RandomAccessNodeImp predecessor;

    /**
     * @param mapOutputSpecs these accesses are needed as outputs for the {@code map()} function.
     * @param cols           these indices among {@code mapOutputSpecs} are the outputs of this NodeImp
     * @param mapperFactory
     */
    RandomAccessNodeImpMap(//
        final AccessImp[] inputs, //
        final RandomAccessNodeImp predecessor, //
        final List<DataSpec> mapOutputSpecs, //
        final int[] cols, //
        final MapperFactory mapperFactory) {

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
    public void moveTo(final long row) {
        predecessor.moveTo(row);
        mapper.run();
    }

    @Override
    public void close() throws IOException {
        predecessor.close();
    }
}
