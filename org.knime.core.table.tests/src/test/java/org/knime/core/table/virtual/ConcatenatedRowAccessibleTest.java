/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
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
import org.knime.core.table.row.Selection;
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
        when(table1.createCursor(Selection.all())).thenReturn(cursor1);
        when(table1.getSchema()).thenReturn(schema);
        var table2 = mock(RowAccessible.class);
        LookaheadCursor<ReadAccessRow> cursor2 = mock(LookaheadCursor.class);
        when(table2.createCursor()).thenReturn(cursor2);
        when(table2.createCursor(Selection.all())).thenReturn(cursor2);
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
