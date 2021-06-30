package org.knime.core.table.virtual.spec;

import java.util.function.BiConsumer;

import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.WriteAccessRow;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * Factory for mappers.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public interface MapperSpec {
    /**
     * @return a mapper. Can be stateful.
     */
    BiConsumer<ReadAccessRow, WriteAccessRow> create();

    // TODO return ColumnSelection object which can provide different implementations depending on the nature of the selection (wide table, many columns selected, all columns selected etc).
    // See also ColumnSelection in org.knime.core.columnar (should be generalized)
    /**
     * @return the indices of the input row the mapper operates on. <source> null </source> if all is selected.
     */
    int[] getColumnSelection();

    /**
     * @return the schema of the output.
     */
    ColumnarSchema getOutputSchema();
}