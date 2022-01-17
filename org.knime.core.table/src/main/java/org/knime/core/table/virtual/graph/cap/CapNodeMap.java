package org.knime.core.table.virtual.graph.cap;

import java.util.Arrays;
import java.util.List;

import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.virtual.spec.MapTransformSpec;

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
    private final MapTransformSpec.Map map;

    public CapNodeMap(final int index, final CapAccessId[] inputs, final int predecessor, List<DataSpec> mapOutputSpecs,
            int[] cols, MapTransformSpec.Map map) {
        super(index, CapNodeType.MAP);
        this.inputs = inputs;
        this.predecessor = predecessor;
        this.mapOutputSpecs = mapOutputSpecs;
        this.cols = cols;
        this.map = map;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("MAP(");
        sb.append("inputs=").append(Arrays.toString(inputs));
        sb.append(", predecessor=").append(predecessor);
        sb.append(", mapOutputSpecs=").append(mapOutputSpecs);
        sb.append(", cols=").append(Arrays.toString(cols));
        sb.append(", map=").append(map);
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
     * consumed downstream. {@code cols()} selects among the of the columns produced by
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
     * @return the map function
     */
    public MapTransformSpec.Map map() {
        return map;
    }
}
