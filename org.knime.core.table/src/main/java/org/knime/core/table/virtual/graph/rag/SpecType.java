package org.knime.core.table.virtual.graph.rag;

import org.knime.core.table.virtual.spec.AppendMapTransformSpec;
import org.knime.core.table.virtual.spec.AppendMissingValuesTransformSpec;
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


// TODO (TP):
//  COLSELECT, APPENDMISSING, APPENDMAP never make it into the TableTransformGraph.
//  Should we split SpecType and NodeType? RawSpecType, NodeSpecType, ... ?
public enum SpecType {
    SOURCE, //
    SLICE, //
    APPEND, //
    APPENDMAP, //
    APPENDMISSING, //
    CONCATENATE, //
    COLSELECT, //
    MAP, //
    ROWFILTER, //
    ROWINDEX, //
    OBSERVER;

    public static SpecType forSpec(final TableTransformSpec spec) {
        if (spec instanceof SourceTransformSpec)
            return SOURCE;
        else if (spec instanceof SliceTransformSpec)
            return SLICE;
        else if (spec instanceof SelectColumnsTransformSpec)
            return COLSELECT;
        else if (spec instanceof AppendTransformSpec)
            return APPEND;
        else if (spec instanceof AppendMissingValuesTransformSpec)
            return APPENDMISSING;
        else if (spec instanceof AppendMapTransformSpec)
            return APPENDMAP;
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
        else
            throw new IllegalArgumentException("TableTransformSpec " + spec + ": spec type not handled (yet)");
    }
}
