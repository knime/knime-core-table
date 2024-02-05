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
 *   Created on 8 Nov 2021 by Steffen Fissler, KNIME GmbH, Konstanz, Germany
 */
package org.knime.core.table.access;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.knime.core.table.access.BooleanAccess.BooleanReadAccess;
import org.knime.core.table.access.BooleanAccess.BooleanWriteAccess;
import org.knime.core.table.access.ByteAccess.ByteReadAccess;
import org.knime.core.table.access.ByteAccess.ByteWriteAccess;
import org.knime.core.table.access.DelegatingWriteAccesses.DelegatingWriteAccess;
import org.knime.core.table.access.DoubleAccess.DoubleReadAccess;
import org.knime.core.table.access.DoubleAccess.DoubleWriteAccess;
import org.knime.core.table.access.FloatAccess.FloatReadAccess;
import org.knime.core.table.access.FloatAccess.FloatWriteAccess;
import org.knime.core.table.access.IntAccess.IntReadAccess;
import org.knime.core.table.access.IntAccess.IntWriteAccess;
import org.knime.core.table.access.ListAccess.ListReadAccess;
import org.knime.core.table.access.ListAccess.ListWriteAccess;
import org.knime.core.table.access.LongAccess.LongReadAccess;
import org.knime.core.table.access.LongAccess.LongWriteAccess;
import org.knime.core.table.access.StringAccess.StringReadAccess;
import org.knime.core.table.access.StringAccess.StringWriteAccess;
import org.knime.core.table.access.StructAccess.StructReadAccess;
import org.knime.core.table.access.StructAccess.StructWriteAccess;
import org.knime.core.table.access.VarBinaryAccess.VarBinaryReadAccess;
import org.knime.core.table.access.VarBinaryAccess.VarBinaryWriteAccess;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.ListDataSpec;
import org.knime.core.table.schema.StructDataSpec;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectSerializer;

/**
 *
 * @author Steffen Fissler, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("javadoc")
public class DelegatingWriteAccessesTest {
    @Test
    public void testBoolean() {
        // Constructor Test
        var delegatingBooleanWriteAccess =
            (BooleanWriteAccess)DelegatingWriteAccesses.createDelegatingWriteAccess(DataSpec.booleanSpec());
        // Mock Setup
        var booleanWriteAccess = mock(BooleanWriteAccess.class);
        var booleanReadAccess = mock(BooleanReadAccess.class);
        // setDelegate Test
        ((DelegatingWriteAccess)delegatingBooleanWriteAccess).setDelegateAccess(booleanWriteAccess);
        assertThat(delegatingBooleanWriteAccess.toString()).isNotNull(); // This asserts that the access is set. If it is not set, we would get a NPE here.

        // setFrom Test
        delegatingBooleanWriteAccess.setFrom(booleanReadAccess);
        verify(booleanWriteAccess).setFrom(booleanReadAccess);

        // setMissing Test
        delegatingBooleanWriteAccess.setMissing();
        verify(booleanWriteAccess).setMissing();

        // setBooleanValue Test
        delegatingBooleanWriteAccess.setBooleanValue(true);
        verify(booleanWriteAccess).setBooleanValue(true);
    }

    @Test
    public void testByte() {
        // Constructor Test
        var delegatingByteWriteAccess =
            (ByteWriteAccess)DelegatingWriteAccesses.createDelegatingWriteAccess(DataSpec.byteSpec());
        // Mock Setup
        var byteWriteAccess = mock(ByteWriteAccess.class);
        var byteReadAccess = mock(ByteReadAccess.class);
        // setDelegate Test
        ((DelegatingWriteAccess)delegatingByteWriteAccess).setDelegateAccess(byteWriteAccess);
        assertThat(delegatingByteWriteAccess.toString()).isNotNull(); // This asserts that the access is set. If it is not set, we would get a NPE here.

        // setFrom Test
        delegatingByteWriteAccess.setFrom(byteReadAccess);
        verify(byteWriteAccess).setFrom(byteReadAccess);

        // setMissing Test
        delegatingByteWriteAccess.setMissing();
        verify(byteWriteAccess).setMissing();

        // setByteValue Test
        delegatingByteWriteAccess.setByteValue((byte)5);
        verify(byteWriteAccess).setByteValue((byte)5);
    }

    @Test
    public void testDouble() {
        // Constructor Test
        var delegatingDoubleWriteAccess =
            (DoubleWriteAccess)DelegatingWriteAccesses.createDelegatingWriteAccess(DataSpec.doubleSpec());
        // Mock Setup
        var doubleWriteAccess = mock(DoubleWriteAccess.class);
        var doubleReadAccess = mock(DoubleReadAccess.class);
        // setDelegate Test
        ((DelegatingWriteAccess)delegatingDoubleWriteAccess).setDelegateAccess(doubleWriteAccess);
        assertThat(delegatingDoubleWriteAccess.toString()).isNotNull(); // This asserts that the access is set. If it is not set, we would get a NPE here.

        // setFrom Test
        delegatingDoubleWriteAccess.setFrom(doubleReadAccess);
        verify(doubleWriteAccess).setFrom(doubleReadAccess);

        // setMissing Test
        delegatingDoubleWriteAccess.setMissing();
        verify(doubleWriteAccess).setMissing();

        // setDoubleValue Test
        delegatingDoubleWriteAccess.setDoubleValue(.5);
        verify(doubleWriteAccess).setDoubleValue(.5);
    }

    @Test
    public void testFloat() {
        // Constructor Test
        var delegatingFloatWriteAccess =
            (FloatWriteAccess)DelegatingWriteAccesses.createDelegatingWriteAccess(DataSpec.floatSpec());
        // Mock Setup
        var floatWriteAccess = mock(FloatWriteAccess.class);
        var floatReadAccess = mock(FloatReadAccess.class);
        // setDelegate Test
        ((DelegatingWriteAccess)delegatingFloatWriteAccess).setDelegateAccess(floatWriteAccess);
        assertThat(delegatingFloatWriteAccess.toString()).isNotNull(); // This asserts that the access is set. If it is not set, we would get a NPE here.

        // setFrom Test
        delegatingFloatWriteAccess.setFrom(floatReadAccess);
        verify(floatWriteAccess).setFrom(floatReadAccess);

        // setMissing Test
        delegatingFloatWriteAccess.setMissing();
        verify(floatWriteAccess).setMissing();

        // setFloatValue Test
        delegatingFloatWriteAccess.setFloatValue(5f);
        verify(floatWriteAccess).setFloatValue(5f);
    }

    @Test
    public void testInt() {
        // Constructor Test
        var delegatingIntWriteAccess =
            (IntWriteAccess)DelegatingWriteAccesses.createDelegatingWriteAccess(DataSpec.intSpec());
        // Mock Setup
        var intWriteAccess = mock(IntWriteAccess.class);
        var intReadAccess = mock(IntReadAccess.class);
        // setDelegate Test
        ((DelegatingWriteAccess)delegatingIntWriteAccess).setDelegateAccess(intWriteAccess);
        assertThat(delegatingIntWriteAccess.toString()).isNotNull(); // This asserts that the access is set. If it is not set, we would get a NPE here.

        // setFrom Test
        delegatingIntWriteAccess.setFrom(intReadAccess);
        verify(intWriteAccess).setFrom(intReadAccess);

        // setMissing Test
        delegatingIntWriteAccess.setMissing();
        verify(intWriteAccess).setMissing();

        // setIntValue Test
        delegatingIntWriteAccess.setIntValue(5);
        verify(intWriteAccess).setIntValue(5);
    }

    @Test
    public void testList() {
        // Constructor Test
        var delegatingListWriteAccess = (ListWriteAccess)DelegatingWriteAccesses
            .createDelegatingWriteAccess(new ListDataSpec(DataSpec.stringSpec()));
        // Mock Setup
        var listWriteAccess = mock(ListWriteAccess.class);
        var listReadAccess = mock(ListReadAccess.class);

        // getWriteAccess 1st test
        assertThat(delegatingListWriteAccess.<WriteAccess> getWriteAccess()).isInstanceOf(StringWriteAccess.class);

        // setDelegate Test
        ((DelegatingWriteAccess)delegatingListWriteAccess).setDelegateAccess(listWriteAccess);
        assertThat(delegatingListWriteAccess.toString()).isNotNull(); // This asserts that the access is set. If it is not set, we would get a NPE here.

        // setFrom Test
        delegatingListWriteAccess.setFrom(listReadAccess);
        verify(listWriteAccess).setFrom(listReadAccess);

        // setMissing Test
        delegatingListWriteAccess.setMissing();
        verify(listWriteAccess).setMissing();

        // create Test
        delegatingListWriteAccess.create(10);
        verify(listWriteAccess).create(10);

        // setWriteIndex Test
        delegatingListWriteAccess.setWriteIndex(5);
        // verify
        verify(listWriteAccess).setWriteIndex(5);

        // getWriteAccess Test
        var writeAccess = delegatingListWriteAccess.getWriteAccess();
        var listWriteAccess2 = mock(ListWriteAccess.class);
        var stringAccess = mock(StringWriteAccess.class);
        when(listWriteAccess2.getWriteAccess()).thenReturn(stringAccess);
        ((DelegatingWriteAccess)delegatingListWriteAccess).setDelegateAccess(listWriteAccess2);
        var stringWriteAccess2 = delegatingListWriteAccess.getWriteAccess();
        assertThat(stringWriteAccess2).isSameAs(writeAccess);
        ((StringWriteAccess)delegatingListWriteAccess.getWriteAccess()).setStringValue("foo");
        verify(((StringWriteAccess)listWriteAccess2.getWriteAccess())).setStringValue("foo");
    }

    @Test
    public void testLong() {
        // Constructor Test
        var delegatingLongWriteAccess =
            (LongWriteAccess)DelegatingWriteAccesses.createDelegatingWriteAccess(DataSpec.longSpec());
        // Mock Setup
        var longWriteAccess = mock(LongWriteAccess.class);
        var longReadAccess = mock(LongReadAccess.class);
        // setDelegate Test
        ((DelegatingWriteAccess)delegatingLongWriteAccess).setDelegateAccess(longWriteAccess);
        assertThat(delegatingLongWriteAccess.toString()).isNotNull(); // This asserts that the access is set. If it is not set, we would get a NPE here.

        // setFrom Test
        delegatingLongWriteAccess.setFrom(longReadAccess);
        verify(longWriteAccess).setFrom(longReadAccess);

        // setMissing Test
        delegatingLongWriteAccess.setMissing();
        verify(longWriteAccess).setMissing();

        // setLongValue Test
        delegatingLongWriteAccess.setLongValue(5l);
        verify(longWriteAccess).setLongValue(5l);
    }

    @Test
    public void testString() {
        // Constructor Test
        var delegatingStringWriteAccess =
            (StringWriteAccess)DelegatingWriteAccesses.createDelegatingWriteAccess(DataSpec.stringSpec());
        // Mock Setup
        var stringWriteAccess = mock(StringWriteAccess.class);
        var stringReadAccess = mock(StringReadAccess.class);
        // setDelegate Test
        ((DelegatingWriteAccess)delegatingStringWriteAccess).setDelegateAccess(stringWriteAccess);
        assertThat(delegatingStringWriteAccess.toString()).isNotNull(); // This asserts that the access is set. If it is not set, we would get a NPE here.

        // setFrom Test
        delegatingStringWriteAccess.setFrom(stringReadAccess);
        verify(stringWriteAccess).setFrom(stringReadAccess);

        // setMissing Test
        delegatingStringWriteAccess.setMissing();
        verify(stringWriteAccess).setMissing();

        // setStringValue Test
        delegatingStringWriteAccess.setStringValue("Ho Ho");
        verify(stringWriteAccess).setStringValue("Ho Ho");
    }

    @Test
    public void testStruct() {
        // Constructor Test
        final var spec = new StructDataSpec(DataSpec.stringSpec(), DataSpec.intSpec());
        var delegatingStructWriteAccess = (StructWriteAccess)DelegatingWriteAccesses.createDelegatingWriteAccess(spec);
        // Size Test
        var structSize = 2;
        assertEquals(structSize, delegatingStructWriteAccess.size());
        // Mock Setup
        var mockStructWriteAccess = mock(StructWriteAccess.class);
        var mockStructReadAccess = mock(StructReadAccess.class);
        var mockStringWriteAccess = mock(StringWriteAccess.class);
        var mockIntWriteAccess = mock(IntWriteAccess.class);
        when(mockStructWriteAccess.getWriteAccess(0)).thenReturn(mockStringWriteAccess);
        when(mockStructWriteAccess.getWriteAccess(1)).thenReturn(mockIntWriteAccess);

        // getWriteAccess 1st test
        assertThat(delegatingStructWriteAccess.<WriteAccess> getWriteAccess(0)).isInstanceOf(StringWriteAccess.class);

        // setDelegate Test
        ((DelegatingWriteAccess)delegatingStructWriteAccess).setDelegateAccess(mockStructWriteAccess);
        assertThat(delegatingStructWriteAccess.toString()).isNotNull(); // This asserts that the access is set. If it is not set, we would get a NPE here.

        // setFrom Test
        delegatingStructWriteAccess.setFrom(mockStructReadAccess);
        verify(mockStructWriteAccess).setFrom(mockStructReadAccess);

        // setMissing Test
        delegatingStructWriteAccess.setMissing();
        verify(mockStructWriteAccess).setMissing();

        // getWriteAccess Test
        ((StringWriteAccess)delegatingStructWriteAccess.getWriteAccess(0)).setStringValue("Yo");
        verify(mockStringWriteAccess).setStringValue("Yo");
        ((IntWriteAccess)delegatingStructWriteAccess.getWriteAccess(1)).setIntValue(5);
        verify(mockIntWriteAccess).setIntValue(5);

        var mock2StringWriteAccess = mock(StringWriteAccess.class);
        var mock2StructWriteAccess = mock(StructWriteAccess.class);
        var stringWriteAccessBeforeSetDelegate = delegatingStructWriteAccess.getWriteAccess(0);
        when(mock2StructWriteAccess.getWriteAccess(0)).thenReturn(mock2StringWriteAccess);
        ((DelegatingWriteAccess)delegatingStructWriteAccess).setDelegateAccess(mock2StructWriteAccess);
        var stringWriteAccessAfterSetDelegate = delegatingStructWriteAccess.getWriteAccess(0);
        assertThat(stringWriteAccessAfterSetDelegate).isSameAs(stringWriteAccessBeforeSetDelegate);
        ((StringWriteAccess)delegatingStructWriteAccess.getWriteAccess(0)).setStringValue("foo");
        verify((StringWriteAccess)mock2StructWriteAccess.getWriteAccess(0)).setStringValue("foo");

    }

    @SuppressWarnings("unchecked")
    @Test
    public void testVarBinary() {
        // Constructor Test
        var delegatingVarBinaryWriteAccess =
            (VarBinaryWriteAccess)DelegatingWriteAccesses.createDelegatingWriteAccess(DataSpec.varBinarySpec());
        // Mock Setup
        var varBinaryWriteAccess = mock(VarBinaryWriteAccess.class);
        var varBinaryReadAccess = mock(VarBinaryReadAccess.class);
        // setDelegate Test
        ((DelegatingWriteAccess)delegatingVarBinaryWriteAccess).setDelegateAccess(varBinaryWriteAccess);
        assertThat(delegatingVarBinaryWriteAccess.toString()).isNotNull(); // This asserts that the access is set. If it is not set, we would get a NPE here.

        // setFrom Test
        delegatingVarBinaryWriteAccess.setFrom(varBinaryReadAccess);
        verify(varBinaryWriteAccess).setFrom(varBinaryReadAccess);

        // setMissing Test
        delegatingVarBinaryWriteAccess.setMissing();
        verify(varBinaryWriteAccess).setMissing();

        // setByteArray Test 1
        delegatingVarBinaryWriteAccess.setByteArray(new byte[]{0, 1});
        verify(varBinaryWriteAccess).setByteArray(new byte[]{0, 1});

        // setByteArray Test 2
        delegatingVarBinaryWriteAccess.setByteArray(new byte[]{0, 1}, 5, 10);
        verify(varBinaryWriteAccess).setByteArray(new byte[]{0, 1}, 5, 10);

        // setObject Test
        var serializer = mock(ObjectSerializer.class);
        delegatingVarBinaryWriteAccess.setObject(varBinaryReadAccess, serializer); // could be any object instad of varBinaryWriteAccess
        verify(varBinaryWriteAccess).setObject(varBinaryReadAccess, serializer);
    }

}
