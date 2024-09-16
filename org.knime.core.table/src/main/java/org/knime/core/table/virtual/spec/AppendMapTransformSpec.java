package org.knime.core.table.virtual.spec;

import java.util.Arrays;

import org.knime.core.table.virtual.spec.MapTransformSpec.MapperFactory;

public class AppendMapTransformSpec implements TableTransformSpec {

    private final int[] inputColumnIndices;

    private final MapperFactory mapperFactory;

    public AppendMapTransformSpec(final int[] columnIndices, final MapperFactory mapperFactory) {
        this.inputColumnIndices = columnIndices;
        this.mapperFactory = mapperFactory;
    }

    /**
     * @return The (input) column indices required for the map computation.
     */
    public int[] getColumnSelection() {
        return inputColumnIndices.clone();
    }

    /**
     * Get the factory used to create mappers. Mappers accept the {@link
     * #getColumnSelection() selected columns} as inputs and produces outputs
     * according to {@link MapperFactory#getOutputSchema()}.
     *
     * @return the MapperFactory
     */
    public MapperFactory getMapperFactory() {
        return mapperFactory;
    }

    @Override
    public String toString() {
        return "AppendMap " + Arrays.toString(inputColumnIndices);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AppendMapTransformSpec that)) {
            return false;
        }

        if (!Arrays.equals(inputColumnIndices, that.inputColumnIndices)) {
            return false;
        }
        return mapperFactory.equals(that.mapperFactory);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(inputColumnIndices);
        result = 31 * result + mapperFactory.hashCode();
        return result;
    }

    public MapTransformSpec toMap() {
        return new MapTransformSpec(inputColumnIndices, mapperFactory);
    }

}
