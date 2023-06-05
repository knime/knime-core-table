package org.knime.core.table.virtual.graph.rag;

import org.knime.core.table.virtual.spec.AppendMissingValuesTransformSpec;
import org.knime.core.table.virtual.spec.AppendTransformSpec;
import org.knime.core.table.virtual.spec.ObserverTransformSpec;
import org.knime.core.table.virtual.spec.SelectColumnsTransformSpec;
import org.knime.core.table.virtual.spec.ConcatenateTransformSpec;
import org.knime.core.table.virtual.spec.IdentityTransformSpec;
import org.knime.core.table.virtual.spec.MapTransformSpec;
import org.knime.core.table.virtual.spec.MaterializeTransformSpec;
import org.knime.core.table.virtual.spec.RowFilterTransformSpec;
import org.knime.core.table.virtual.spec.SliceTransformSpec;
import org.knime.core.table.virtual.spec.SourceTransformSpec;
import org.knime.core.table.virtual.spec.TableTransformSpec;

public enum RagNodeType {
    SOURCE, //
    SLICE, //
    APPEND, //
    APPENDMISSING, //
    MISSING, //
    CONCATENATE, //
    COLFILTER, //
    MAP, //
    ROWFILTER, //
    CONSUMER, //
    MATERIALIZE, //
    WRAPPER, //
    IDENTITY, //
    ROWINDEX, //
    OBSERVER;

    public static RagNodeType forSpec(final TableTransformSpec spec) {
        if (spec instanceof SourceTransformSpec)
            return RagNodeType.SOURCE;
        else if (spec instanceof MissingValuesSourceTransformSpec)
            return RagNodeType.MISSING;
        else if (spec instanceof SliceTransformSpec)
            return RagNodeType.SLICE;
        else if (spec instanceof SelectColumnsTransformSpec)
            return RagNodeType.COLFILTER;
        else if (spec instanceof AppendMissingValuesTransformSpec)
            return RagNodeType.APPENDMISSING;
        else if (spec instanceof AppendTransformSpec)
            return RagNodeType.APPEND;
        else if (spec instanceof ConcatenateTransformSpec)
            return RagNodeType.CONCATENATE;
        else if (spec instanceof MapTransformSpec)
            return RagNodeType.MAP;
        else if (spec instanceof RowFilterTransformSpec)
            return RagNodeType.ROWFILTER;
        else if (spec instanceof ConsumerTransformSpec)
            return RagNodeType.CONSUMER;
        else if (spec instanceof MaterializeTransformSpec)
            return RagNodeType.MATERIALIZE;
        else if (spec instanceof WrapperTransformSpec)
            return RagNodeType.WRAPPER;
        else if (spec instanceof IdentityTransformSpec)
            return RagNodeType.IDENTITY;
        else if (spec instanceof RowIndexTransformSpec)
            return RagNodeType.ROWINDEX;
        else if (spec instanceof ObserverTransformSpec)
            return RagNodeType.OBSERVER;
        else
            throw new IllegalArgumentException();
    }
}
