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
 *   Created on Jul 27, 2021 by Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
package org.knime.core.table.cursor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.knime.core.table.access.IntAccess.IntReadAccess;
import org.knime.core.table.access.StringAccess.StringReadAccess;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.DefaultColumnarSchema;
import org.knime.core.table.schema.traits.DataTraits;
import org.knime.core.table.schema.traits.DefaultDataTraits;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Contains unit tests for {@link BufferingLookaheadCursor}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("javadoc")
@RunWith(MockitoJUnitRunner.class)
public class BufferingLookaheadCursorTest {

    private static final DataSpec[] SPECS =
        {DataSpec.stringSpec(), DataSpec.intSpec()};

    private static final DataTraits[] TRAITS = {DefaultDataTraits.EMPTY, DefaultDataTraits.EMPTY};

    private static final ColumnarSchema SCHEMA = new DefaultColumnarSchema(SPECS, TRAITS);

    @Mock
    private Cursor<ReadAccessRow> m_underlyingCursor;

    @Mock
    private ReadAccessRow m_readAccessRow;

    @Mock
    private StringReadAccess m_stringReadAccess;

    @Mock
    private IntReadAccess m_intReadAccess;

    private BufferingLookaheadCursor m_testInstance;

    @Before
    public void init() {
        m_testInstance = new BufferingLookaheadCursor(SCHEMA, m_underlyingCursor);
    }

    public void testAccessNotEmptyAfterFirstCanForward() {
        stubAccess();
        stubAccesses("foo", 7);
        when(m_underlyingCursor.forward()).thenReturn(true);
        assertTrue(m_testInstance.canForward());
        var readAccessRow = m_testInstance.access();
        assertTrue(readAccessRow.getAccess(0).isMissing());
    }

    @Test
    public void testNormalIteration() {
        stubAccess();
        stubAccesses("foo", 7);
        final ReadAccessRow access = m_testInstance.access();
        final StringReadAccess stringAccess = access.getAccess(0);
        final IntReadAccess intAccess = access.getAccess(1);
        when(m_underlyingCursor.forward()).thenReturn(true);
        assertTrue(m_testInstance.canForward());
        assertTrue(m_testInstance.forward());
        assertEquals("foo", stringAccess.getStringValue());
        assertEquals(7, intAccess.getIntValue());
        stubAccesses("bar", 42);
        assertTrue(m_testInstance.canForward());
        assertTrue(m_testInstance.forward());
        assertEquals("bar", stringAccess.getStringValue());
        assertEquals(42, intAccess.getIntValue());
        when(m_underlyingCursor.forward()).thenReturn(false);
        assertFalse(m_testInstance.canForward());
        // TODO should the accesses be cleared by the cursor if we reached the end?
        assertFalse(m_testInstance.forward());

        final ReadAccessRow afterIteration = m_testInstance.access();
        assertSame(access, afterIteration);
        assertSame(stringAccess, afterIteration.getAccess(0));
        assertSame(intAccess, afterIteration.getAccess(1));
    }

    @Test
    public void testMultipleCanForwardCalls() {
        stubAccess();
        when(m_underlyingCursor.forward()).thenReturn(true);
        stubAccesses("foo", 7);
        m_testInstance.canForward();
        stubAccesses("bar", 42);
        m_testInstance.canForward();
        m_testInstance.forward();
        checkAccess("foo", 7);
        m_testInstance.canForward();
        checkAccess("foo", 7);
        m_testInstance.forward();
        checkAccess("bar", 42);
    }

    @Test
    public void testIterateOnlyWithForward() {
        stubAccess();
        when(m_underlyingCursor.forward()).thenReturn(true);
        stubAccesses("bar", 42);
        assertTrue(m_testInstance.forward());
        checkAccess("bar", 42);
        stubAccesses("foo", 13);
        assertTrue(m_testInstance.forward());
        checkAccess("foo", 13);
        when(m_underlyingCursor.forward()).thenReturn(false);
        assertFalse(m_testInstance.forward());
    }

    private void checkAccess(final String stringValue, final int intValue) {
        final ReadAccessRow access = m_testInstance.access();
        final StringReadAccess stringAccess = access.getAccess(0);
        final IntReadAccess intAccess = access.getAccess(1);
        assertEquals(stringValue, stringAccess.getStringValue());
        assertEquals(intValue, intAccess.getIntValue());
    }

    private void stubAccesses(final String stringValue, final int intValue) {
        when(m_stringReadAccess.getStringValue()).thenReturn(stringValue);
        when(m_intReadAccess.getIntValue()).thenReturn(intValue);
    }

    private void stubAccess() {
        when(m_underlyingCursor.access()).thenReturn(m_readAccessRow);
        when(m_readAccessRow.getAccess(0)).thenReturn(m_stringReadAccess);
        when(m_readAccessRow.getAccess(1)).thenReturn(m_intReadAccess);
        when(m_readAccessRow.size()).thenReturn(2);
    }

    @Test
    public void testClose() throws IOException {
        m_testInstance.close();
        verify(m_underlyingCursor).close();
    }
}
