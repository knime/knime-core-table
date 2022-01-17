package org.knime.core.table.virtual.spec;

import java.util.Arrays;

import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.access.WriteAccess;
import org.knime.core.table.schema.ColumnarSchema;

public final class MapTransformSpec implements TableTransformSpec {

    public interface Map {

        void map(final ReadAccess[] inputs, final WriteAccess[] outputs);
    }

    private final int[] inputColumnIndices;
    private final ColumnarSchema outputSchema;
    private final Map map;

    public MapTransformSpec(final int[] columnIndices, final ColumnarSchema outputSchema, final Map map) {
        this.inputColumnIndices = columnIndices;
        this.outputSchema = outputSchema;
        this.map = map;
    }

    public ColumnarSchema getSchema() {
        return outputSchema;
    }

    /**
     * @return The (input) column indices required for the map computation.
     */
    public int[] getColumnSelection() {
        return inputColumnIndices.clone();
    }

    public Map getMap() {
        return map;
    }

    @Override
    public String toString() {
        return "Map " + Arrays.toString(inputColumnIndices);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof MapTransformSpec))
            return false;

        final MapTransformSpec that = (MapTransformSpec)o;
        if (!Arrays.equals(inputColumnIndices, that.inputColumnIndices))
            return false;
        return map.equals(that.map);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(inputColumnIndices);
        result = 31 * result + map.hashCode();
        return result;
    }
}
