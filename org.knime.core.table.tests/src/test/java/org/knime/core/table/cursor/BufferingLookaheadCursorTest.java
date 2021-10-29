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
