/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any questions please contact the copyright holder:
 * website: www.knime.com
 * email: contact@knime.com
 * ---------------------------------------------------------------------
 *
 * History
 *   Created on Oct 29, 2021 by Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
package org.knime.core.table.virtual;

import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;

import org.junit.Test;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.cursor.LookaheadCursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.DefaultColumnarSchema;
import org.knime.core.table.schema.traits.DefaultDataTraits;
import org.mockito.ArgumentMatchers;

/**
 * Unit tests for ConcatenatedRowAccessible.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public class ConcatenatedRowAccessibleTest {

    @SuppressWarnings({"resource", "javadoc", "unchecked"})// no actual resources are allocated
    @Test
    public void testConsumedDelegateCursorsAreClosed() throws IOException {
        var schema = new DefaultColumnarSchema(DataSpec.stringSpec(), DefaultDataTraits.EMPTY);
        var table1 = mock(RowAccessible.class);
        LookaheadCursor<ReadAccessRow> cursor1 = mock(LookaheadCursor.class);
        when(table1.createCursor()).thenReturn(cursor1);
        when(table1.getSchema()).thenReturn(schema);
        var table2 = mock(RowAccessible.class);
        LookaheadCursor<ReadAccessRow> cursor2 = mock(LookaheadCursor.class);
        when(table2.createCursor()).thenReturn(cursor2);
        when(table2.getSchema()).thenReturn(schema);
        var rowRead = mock(ReadAccessRow.class);
        var readAccess = mock(ReadAccess.class);
        when(rowRead.getAccess(ArgumentMatchers.anyInt())).thenReturn(readAccess);
        when(cursor2.access()).thenReturn(rowRead);
        var concatenated = new ConcatenatedRowAccessible(List.of(table1, table2));

        when(cursor1.canForward()).thenReturn(false);
        when(cursor2.canForward()).thenReturn(true);

        var concatenatedCursor = concatenated.createCursor();
        verify(cursor1).close();
        when(cursor2.canForward()).thenReturn(false);
        assertFalse(concatenatedCursor.canForward());
        verify(cursor2).close();
    }
}
