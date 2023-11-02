package org.knime.core.table.virtual.graph.rag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.DataSpecs.DataSpecWithTraits;
import org.knime.core.table.schema.traits.DataTraits;

class MissingValueColumns {

    private final List<DataSpecWithTraits> specs = new ArrayList<>();

    final List<DataSpecWithTraits> unmodifiable = Collections.unmodifiableList(specs);

    /**
     * We keep track of which types of missing-value columns we need. For example, we only need one
     * {@code DoubleDataSpec} column, even if there are multiple missing {@code DoubleDataSpec} columns used throughout
     * a {@code VirtualTable} construction.
     * <p>
     * Newly occurring {@code DataSpec}s are assigned consecutive {@code int} indices. These indices correspond to
     * column indices in the (singleton) MISSING node {@link RagGraph#getMissingValuesSource()}.
     *
     * @param spec a {@code DataSpec} for which a missing-value column is needed.
     * @return column index in {@link RagGraph#getMissingValuesSource() missing-values source}
     */
    public int getOrAdd(final DataSpec spec, final DataTraits traits) {
        var column = new DataSpecWithTraits(spec, traits);
        int i = specs.indexOf(column);
        if (i < 0) {
            i = specs.size();
            specs.add(column);
        }
        return i;
    }

}
