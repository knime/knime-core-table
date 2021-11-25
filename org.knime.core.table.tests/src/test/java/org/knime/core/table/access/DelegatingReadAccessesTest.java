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
 *   Created on 14 Oct 2021 by Steffen Fissler, KNIME GmbH, Konstanz, Germany
 */
package org.knime.core.table.access;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.junit.Test;
import org.knime.core.table.access.BooleanAccess.BooleanReadAccess;
import org.knime.core.table.access.ByteAccess.ByteReadAccess;
import org.knime.core.table.access.DelegatingReadAccesses.DelegatingReadAccess;
import org.knime.core.table.access.DoubleAccess.DoubleReadAccess;
import org.knime.core.table.access.FloatAccess.FloatReadAccess;
import org.knime.core.table.access.IntAccess.IntReadAccess;
import org.knime.core.table.access.ListAccess.ListReadAccess;
import org.knime.core.table.access.LongAccess.LongReadAccess;
import org.knime.core.table.access.StringAccess.StringReadAccess;
import org.knime.core.table.access.StructAccess.StructReadAccess;
import org.knime.core.table.access.VarBinaryAccess.VarBinaryReadAccess;
import org.knime.core.table.io.ReadableDataInput;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.ListDataSpec;
import org.knime.core.table.schema.StructDataSpec;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectDeserializer;

/**
 *
 * @author Steffen Fissler, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("javadoc")
public class DelegatingReadAccessesTest {

    @Test
    public void testInt() {
        // Constructor Test
        var delegatingIntReadAccess = (IntReadAccess)DelegatingReadAccesses.createDelegatingAccess(DataSpec.intSpec());
        // Spec Test
        assertThat(delegatingIntReadAccess.getDataSpec()).isEqualTo(DataSpec.intSpec());
        // 1. isMissing Test
        assertThat(delegatingIntReadAccess.isMissing()).isTrue(); // m_delegateAccesss == null, thus is missing
        // Mock Setup
        var intReadAccess = mock(IntReadAccess.class);
        // Set Test
        ((DelegatingReadAccess)delegatingIntReadAccess).setDelegateAccess(intReadAccess);
        // 2. isMissing Test
        assertThat(delegatingIntReadAccess.isMissing()).isFalse();
        // getIntValue Test
        when(intReadAccess.getIntValue()).thenReturn(59);
        assertThat(delegatingIntReadAccess.getIntValue()).isEqualTo(59);
        verify(intReadAccess).getIntValue();
    }

    @Test
    public void testList() {
        // Constructor Test
        var delegatingListReadAccess =
            (ListReadAccess)DelegatingReadAccesses.createDelegatingAccess(new ListDataSpec(DataSpec.stringSpec()));
        // Size Test
        assertEquals(0, delegatingListReadAccess.size());

        // setDelegateAccess
        var listReadAccess = mock(ListReadAccess.class);
        var stringReadAccess = mock(StringReadAccess.class);
        when(listReadAccess.getAccess()).thenReturn(stringReadAccess);
        ((DelegatingReadAccess)delegatingListReadAccess).setDelegateAccess(listReadAccess);

        // getAccess Test
        when(stringReadAccess.getStringValue()).thenReturn("Hui Buh");
        var stringOfFirstElement = ((StringReadAccess)delegatingListReadAccess.getAccess()).getStringValue();
        assertThat(stringOfFirstElement).isEqualTo("Hui Buh");

        // setIndex Test
        delegatingListReadAccess.setIndex(1);
        when(stringReadAccess.getStringValue()).thenReturn("Hui Bui");
        var stringOfSecondElement = ((StringReadAccess)delegatingListReadAccess.getAccess()).getStringValue();
        assertThat(stringOfSecondElement).isEqualTo("Hui Bui");

        // isMissing List
        assertFalse(delegatingListReadAccess.isMissing());

        // isMissing inner
        assertFalse(delegatingListReadAccess.isMissing(0));

        // Spec Test outer
        assertEquals(new ListDataSpec(DataSpec.stringSpec()), delegatingListReadAccess.getDataSpec());
    }

    @Test
    public void testBoolean() {
        // Constructor Test
        var delegatingBooleanReadAccess =
            (BooleanReadAccess)DelegatingReadAccesses.createDelegatingAccess(DataSpec.booleanSpec());
        // Spec Test
        assertEquals(DataSpec.booleanSpec(), delegatingBooleanReadAccess.getDataSpec());
        // 1. isMissing Test
        assertTrue(delegatingBooleanReadAccess.isMissing()); // m_delegateAccesss == null, thus is missing
        // Mock Setup
        var booleanReadAccess = mock(BooleanReadAccess.class);
        // Set Test
        ((DelegatingReadAccess)delegatingBooleanReadAccess).setDelegateAccess(booleanReadAccess);
        // 2. isMissing Test
        assertFalse(delegatingBooleanReadAccess.isMissing());
        // getBooleanValue Test
        when(booleanReadAccess.getBooleanValue()).thenReturn(true);
        assertEquals(true, delegatingBooleanReadAccess.getBooleanValue());
        verify(booleanReadAccess).getBooleanValue();
    }

    @Test
    public void testByte() {
        // Constructor Test
        var delegatingByteReadAccess =
            (ByteReadAccess)DelegatingReadAccesses.createDelegatingAccess(DataSpec.byteSpec());
        // Spec Test
        assertEquals(DataSpec.byteSpec(), delegatingByteReadAccess.getDataSpec());
        // 1. isMissing Test
        assertTrue(delegatingByteReadAccess.isMissing()); // m_delegateAccesss == null, thus is missing
        // Mock Setup
        var byteReadAccess = mock(ByteReadAccess.class);
        // Set Test
        ((DelegatingReadAccess)delegatingByteReadAccess).setDelegateAccess(byteReadAccess);
        // 2. isMissing Test
        assertFalse(delegatingByteReadAccess.isMissing());
        // getByteValue Test
        when(byteReadAccess.getByteValue()).thenReturn((byte)42);
        assertEquals((byte)42, delegatingByteReadAccess.getByteValue());
        verify(byteReadAccess).getByteValue();
    }

    @Test
    public void testDouble() {
        // Constructor Test
        var delegatingDoubleReadAccess =
            (DoubleReadAccess)DelegatingReadAccesses.createDelegatingAccess(DataSpec.doubleSpec());
        // Spec Test
        assertEquals(DataSpec.doubleSpec(), delegatingDoubleReadAccess.getDataSpec());
        // 1. isMissing Test
        assertTrue(delegatingDoubleReadAccess.isMissing()); // m_delegateAccesss == null, thus is missing
        // Mock Setup
        var doubleReadAccess = mock(DoubleReadAccess.class);
        // Set Test
        ((DelegatingReadAccess)delegatingDoubleReadAccess).setDelegateAccess(doubleReadAccess);
        // 2. isMissing Test
        assertFalse(delegatingDoubleReadAccess.isMissing());
        // getDoubleValue Test
        when(doubleReadAccess.getDoubleValue()).thenReturn(.42);
        assertEquals(.42, delegatingDoubleReadAccess.getDoubleValue(), 0.0001);
        verify(doubleReadAccess).getDoubleValue();
    }

    @Test
    public void testFloat() {
        // Constructor Test
        var delegatingFloatReadAccess =
            (FloatReadAccess)DelegatingReadAccesses.createDelegatingAccess(DataSpec.floatSpec());
        // Spec Test
        assertEquals(DataSpec.floatSpec(), delegatingFloatReadAccess.getDataSpec());
        // 1. isMissing Test
        assertTrue(delegatingFloatReadAccess.isMissing()); // m_delegateAccesss == null, thus is missing
        // Mock Setup
        var floatReadAccess = mock(FloatReadAccess.class);
        // Set Test
        ((DelegatingReadAccess)delegatingFloatReadAccess).setDelegateAccess(floatReadAccess);
        // 2. isMissing Test
        assertFalse(delegatingFloatReadAccess.isMissing());
        // getFloatValue Test
        when(floatReadAccess.getFloatValue()).thenReturn(.42f);
        assertEquals(.42f, delegatingFloatReadAccess.getFloatValue(), 0.0001f);
        verify(floatReadAccess).getFloatValue();
    }

    @Test
    public void testLong() {
        // Constructor Test
        var delegatingLongReadAccess =
            (LongReadAccess)DelegatingReadAccesses.createDelegatingAccess(DataSpec.longSpec());
        // Spec Test
        assertEquals(DataSpec.longSpec(), delegatingLongReadAccess.getDataSpec());
        // 1. isMissing Test
        assertTrue(delegatingLongReadAccess.isMissing()); // m_delegateAccesss == null, thus is missing
        // Mock Setup
        var longReadAccess = mock(LongReadAccess.class);
        // Set Test
        ((DelegatingReadAccess)delegatingLongReadAccess).setDelegateAccess(longReadAccess);
        // 2. isMissing Test
        assertFalse(delegatingLongReadAccess.isMissing());
        // getLongValue Test
        when(longReadAccess.getLongValue()).thenReturn(42l);
        assertEquals(42l, delegatingLongReadAccess.getLongValue());
        verify(longReadAccess).getLongValue();
    }

    @Test
    public void testString() {
        // Constructor Test
        var delegatingStringReadAccess =
            (StringReadAccess)DelegatingReadAccesses.createDelegatingAccess(DataSpec.stringSpec());
        // Spec Test
        assertEquals(DataSpec.stringSpec(), delegatingStringReadAccess.getDataSpec());
        // 1. isMissing Test
        assertTrue(delegatingStringReadAccess.isMissing()); // m_delegateAccesss == null, thus is missing
        // Mock Setup
        var stringReadAccess = mock(StringReadAccess.class);
        // Set Test
        ((DelegatingReadAccess)delegatingStringReadAccess).setDelegateAccess(stringReadAccess);
        // 2. isMissing Test
        assertFalse(delegatingStringReadAccess.isMissing());
        // getStringValue Test
        when(stringReadAccess.getStringValue()).thenReturn("Hui Buh");
        assertEquals("Hui Buh", delegatingStringReadAccess.getStringValue());
        verify(stringReadAccess).getStringValue();
    }

    @Test
    public void testStruct() {
        final var spec = new StructDataSpec(DataSpec.stringSpec(), DataSpec.intSpec());
        var delegatingStructReadAccess = (StructReadAccess)DelegatingReadAccesses.createDelegatingAccess(spec);
        // Size Test
        assertEquals(2, delegatingStructReadAccess.size());

        // setDelegateAccess
        var structReadAccess = mock(StructReadAccess.class);
        var stringReadAccess = mock(StringReadAccess.class);
        var intReadAccess = mock(IntReadAccess.class);
        when(structReadAccess.getAccess(0)).thenReturn(stringReadAccess);
        when(structReadAccess.getAccess(1)).thenReturn(intReadAccess);
        ((DelegatingReadAccess)delegatingStructReadAccess).setDelegateAccess(structReadAccess);

        // getAccess Test
        when(stringReadAccess.getStringValue()).thenReturn("Hui");
        when(intReadAccess.getIntValue()).thenReturn(42);
        var firstElementOfStruct = ((StringReadAccess)delegatingStructReadAccess.getAccess(0)).getStringValue();
        var secondElementOfStruct = ((IntReadAccess)delegatingStructReadAccess.getAccess(1)).getIntValue();
        assertThat(firstElementOfStruct).isEqualTo("Hui");
        assertThat(secondElementOfStruct).isEqualTo(42);

        // isMissing Struct
        assertFalse(delegatingStructReadAccess.isMissing());

        // Spec Test outer
        assertEquals(spec, delegatingStructReadAccess.getDataSpec());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testVarBinary() {
        // Constructor Test
        var delegatingVarBinaryReadAccess =
            (VarBinaryReadAccess)DelegatingReadAccesses.createDelegatingAccess(DataSpec.varBinarySpec());
        // Spec Test
        assertEquals(DataSpec.varBinarySpec(), delegatingVarBinaryReadAccess.getDataSpec());
        // 1. isMissing Test
        assertTrue(delegatingVarBinaryReadAccess.isMissing()); // m_delegateAccesss == null, thus is missing
        // Mock Setup
        var varBinaryReadAccess = mock(VarBinaryReadAccess.class);
        // Set Test
        ((DelegatingReadAccess)delegatingVarBinaryReadAccess).setDelegateAccess(varBinaryReadAccess);
        // 2. isMissing Test
        assertFalse(delegatingVarBinaryReadAccess.isMissing());
        // getVarBinaryValue Test
        when(varBinaryReadAccess.getByteArray()).thenReturn(new byte[]{0, 1});
        assertArrayEquals(new byte[]{0, 1}, delegatingVarBinaryReadAccess.getByteArray());
        verify(varBinaryReadAccess).getByteArray();
        // getObject Test
        var deserializer = mock(ObjectDeserializer.class);
        ReadableDataInput input = mock(ReadableDataInput.class);
        try {
            when(deserializer.deserialize(input)).thenReturn("Hui Buh");
        } catch (IOException e) {
            fail(e.toString()); // will not happen
        }
        when(varBinaryReadAccess.getObject(deserializer)).thenReturn("Hui Buh");
        assertEquals("Hui Buh", varBinaryReadAccess.getObject(deserializer));
    }
}
