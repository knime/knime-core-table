package org.knime.core.table.row;

import java.util.Arrays;
import java.util.function.IntFunction;

import org.knime.core.table.access.ReadAccess;

public class DefaultReadAccessRow implements ReadAccessRow {

    private final ReadAccess[] accesses;

    public DefaultReadAccessRow(final int size, final IntFunction<? extends ReadAccess> generator) {
        accesses = new ReadAccess[size];
        Arrays.setAll(accesses, generator);
    }

    @Override
    public int size() {
        return accesses.length;
    }

    @Override
    public <A extends ReadAccess> A getAccess(int index) {
        return (A)accesses[index];
    }
}
