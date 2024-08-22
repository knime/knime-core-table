package org.knime.core.table.virtual.graph.rag3;

import org.knime.core.table.virtual.graph.rag.ConsumerTransformSpec;
import org.knime.core.table.virtual.spec.AppendTransformSpec;
import org.knime.core.table.virtual.spec.ConcatenateTransformSpec;
import org.knime.core.table.virtual.spec.MapTransformSpec;
import org.knime.core.table.virtual.spec.ObserverTransformSpec;
import org.knime.core.table.virtual.spec.RowFilterTransformSpec;
import org.knime.core.table.virtual.spec.RowIndexTransformSpec;
import org.knime.core.table.virtual.spec.SelectColumnsTransformSpec;
import org.knime.core.table.virtual.spec.SliceTransformSpec;
import org.knime.core.table.virtual.spec.SourceTransformSpec;
import org.knime.core.table.virtual.spec.TableTransformSpec;

public enum SpecType {
    SOURCE, //
    SLICE, //
    APPEND, //
    CONCATENATE, //
    COLSELECT, //
    MAP, //
    ROWFILTER, //
    ROWINDEX, //
    OBSERVER, //
    CONSUMER;

    public static SpecType forSpec(final TableTransformSpec spec) {
        if (spec instanceof SourceTransformSpec)
            return SOURCE;
        else if (spec instanceof SliceTransformSpec)
            return SLICE;
        else if (spec instanceof SelectColumnsTransformSpec)
            return COLSELECT;
        else if (spec instanceof AppendTransformSpec)
            return APPEND;
        else if (spec instanceof ConcatenateTransformSpec)
            return CONCATENATE;
        else if (spec instanceof MapTransformSpec)
            return MAP;
        else if (spec instanceof RowFilterTransformSpec)
            return ROWFILTER;
        else if (spec instanceof RowIndexTransformSpec)
            return ROWINDEX;
        else if (spec instanceof ObserverTransformSpec)
            return OBSERVER;
        else if (spec instanceof ConsumerTransformSpec)
            return CONSUMER;
        else
            throw new IllegalArgumentException();
    }
}
