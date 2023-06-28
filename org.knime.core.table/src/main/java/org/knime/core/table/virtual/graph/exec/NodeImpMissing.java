package org.knime.core.table.virtual.graph.exec;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.knime.core.table.access.MissingAccesses;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.schema.DataSpec;

class NodeImpMissing implements NodeImp {
    private final ReadAccess[] outputs;

    NodeImpMissing(final List<DataSpec> missingValueSpecs) {
        outputs = new ReadAccess[missingValueSpecs.size()];
        Arrays.setAll(outputs, i -> MissingAccesses.getMissingAccess(missingValueSpecs.get(i)));
    }

    @Override
    public ReadAccess getOutput(final int i) {
        return outputs[i];
    }

    @Override
    public void create() {
        // should never be called
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean forward() {
        // should never be called
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean canForward() {
        // should never be called
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        // should never be called
        throw new UnsupportedOperationException();
    }
}
