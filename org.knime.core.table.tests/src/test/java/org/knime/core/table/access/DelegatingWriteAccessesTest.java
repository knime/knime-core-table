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
 *   Created on 8 Nov 2021 by Steffen Fissler, KNIME GmbH, Konstanz, Germany
 */
package org.knime.core.table.access;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import org.junit.Test;
import org.knime.core.table.access.BooleanAccess.BooleanReadAccess;
import org.knime.core.table.access.BooleanAccess.BooleanWriteAccess;
import org.knime.core.table.access.ByteAccess.ByteReadAccess;
import org.knime.core.table.access.ByteAccess.ByteWriteAccess;
import org.knime.core.table.access.DelegatingWriteAccesses.DelegatingWriteAccess;
import org.knime.core.table.access.DoubleAccess.DoubleReadAccess;
import org.knime.core.table.access.DoubleAccess.DoubleWriteAccess;
import org.knime.core.table.access.DurationAccess.DurationReadAccess;
import org.knime.core.table.access.DurationAccess.DurationWriteAccess;
import org.knime.core.table.access.FloatAccess.FloatReadAccess;
import org.knime.core.table.access.FloatAccess.FloatWriteAccess;
import org.knime.core.table.access.IntAccess.IntReadAccess;
import org.knime.core.table.access.IntAccess.IntWriteAccess;
import org.knime.core.table.access.ListAccess.ListReadAccess;
import org.knime.core.table.access.ListAccess.ListWriteAccess;
import org.knime.core.table.access.LocalDateAccess.LocalDateReadAccess;
import org.knime.core.table.access.LocalDateAccess.LocalDateWriteAccess;
import org.knime.core.table.access.LocalDateTimeAccess.LocalDateTimeReadAccess;
import org.knime.core.table.access.LocalDateTimeAccess.LocalDateTimeWriteAccess;
import org.knime.core.table.access.LocalTimeAccess.LocalTimeReadAccess;
import org.knime.core.table.access.LocalTimeAccess.LocalTimeWriteAccess;
import org.knime.core.table.access.LongAccess.LongReadAccess;
import org.knime.core.table.access.LongAccess.LongWriteAccess;
import org.knime.core.table.access.PeriodAccess.PeriodReadAccess;
import org.knime.core.table.access.PeriodAccess.PeriodWriteAccess;
import org.knime.core.table.access.StringAccess.StringReadAccess;
import org.knime.core.table.access.StringAccess.StringWriteAccess;
import org.knime.core.table.access.StructAccess.StructReadAccess;
import org.knime.core.table.access.StructAccess.StructWriteAccess;
import org.knime.core.table.access.VarBinaryAccess.VarBinaryReadAccess;
import org.knime.core.table.access.VarBinaryAccess.VarBinaryWriteAccess;
import org.knime.core.table.access.ZonedDateTimeAccess.ZonedDateTimeReadAccess;
import org.knime.core.table.access.ZonedDateTimeAccess.ZonedDateTimeWriteAccess;
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
        var delegatingBooleanWriteAccess = (BooleanWriteAccess)DelegatingWriteAccesses.createDelegatingWriteAccess(DataSpec.booleanSpec());
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
        var delegatingByteWriteAccess = (ByteWriteAccess)DelegatingWriteAccesses.createDelegatingWriteAccess(DataSpec.byteSpec());
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
        var delegatingDoubleWriteAccess = (DoubleWriteAccess)DelegatingWriteAccesses.createDelegatingWriteAccess(DataSpec.doubleSpec());
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
        var delegatingFloatWriteAccess = (FloatWriteAccess)DelegatingWriteAccesses.createDelegatingWriteAccess(DataSpec.floatSpec());
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
        var delegatingIntWriteAccess = (IntWriteAccess)DelegatingWriteAccesses.createDelegatingWriteAccess(DataSpec.intSpec());
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
        var delegatingListWriteAccess = (ListWriteAccess)DelegatingWriteAccesses.createDelegatingWriteAccess(new ListDataSpec(DataSpec.stringSpec()));
        // Mock Setup
        var listWriteAccess = mock(ListWriteAccess.class);
        var listReadAccess = mock(ListReadAccess.class);

        // getWriteAccess 1st test
        assertThat(delegatingListWriteAccess.<WriteAccess>getWriteAccess()).isInstanceOf(StringWriteAccess.class);

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
        var delegatingLongWriteAccess = (LongWriteAccess)DelegatingWriteAccesses.createDelegatingWriteAccess(DataSpec.longSpec());
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
        var delegatingStringWriteAccess = (StringWriteAccess)DelegatingWriteAccesses.createDelegatingWriteAccess(DataSpec.stringSpec());
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
        assertThat(delegatingStructWriteAccess.<WriteAccess>getWriteAccess(0)).isInstanceOf(StringWriteAccess.class);

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




    @Test
    public void testDuration() {
        // Constructor Test
        var delegatingDurationWriteAccess = (DurationWriteAccess)DelegatingWriteAccesses.createDelegatingWriteAccess(DataSpec.durationSpec());
        // Mock Setup
        var durationWriteAccess = mock(DurationWriteAccess.class);
        var durationReadAccess = mock(DurationReadAccess.class);
        // setDelegate Test
        ((DelegatingWriteAccess)delegatingDurationWriteAccess).setDelegateAccess(durationWriteAccess);
        assertThat(delegatingDurationWriteAccess.toString()).isNotNull(); // This asserts that the access is set. If it is not set, we would get a NPE here.

        // setFrom Test
        delegatingDurationWriteAccess.setFrom(durationReadAccess);
        verify(durationWriteAccess).setFrom(durationReadAccess);

        // setMissing Test
        delegatingDurationWriteAccess.setMissing();
        verify(durationWriteAccess).setMissing();

        // setDurationValue Test
        delegatingDurationWriteAccess.setDurationValue(Duration.ofDays(2));
        verify(durationWriteAccess).setDurationValue(Duration.ofDays(2));
    }


    @Test
    public void testLocalDate() {
        // Constructor Test
        var delegatingLocalDateWriteAccess = (LocalDateWriteAccess)DelegatingWriteAccesses.createDelegatingWriteAccess(DataSpec.localDateSpec());
        // Mock Setup
        var localDateWriteAccess = mock(LocalDateWriteAccess.class);
        var localDateReadAccess = mock(LocalDateReadAccess.class);
        // setDelegate Test
        ((DelegatingWriteAccess)delegatingLocalDateWriteAccess).setDelegateAccess(localDateWriteAccess);
        assertThat(delegatingLocalDateWriteAccess.toString()).isNotNull(); // This asserts that the access is set. If it is not set, we would get a NPE here.

        // setFrom Test
        delegatingLocalDateWriteAccess.setFrom(localDateReadAccess);
        verify(localDateWriteAccess).setFrom(localDateReadAccess);

        // setMissing Test
        delegatingLocalDateWriteAccess.setMissing();
        verify(localDateWriteAccess).setMissing();

        // setLocalDateValue Test
        delegatingLocalDateWriteAccess.setLocalDateValue(LocalDate.ofYearDay(2021, 32));
        verify(localDateWriteAccess).setLocalDateValue(LocalDate.ofYearDay(2021, 32));
    }


    @Test
    public void testLocalDateTime() {
        // Constructor Test
        var delegatingLocalDateTimeWriteAccess = (LocalDateTimeWriteAccess)DelegatingWriteAccesses.createDelegatingWriteAccess(DataSpec.localDateTimeSpec());
        // Mock Setup
        var localDateTimeWriteAccess = mock(LocalDateTimeWriteAccess.class);
        var localDateTimeReadAccess = mock(LocalDateTimeReadAccess.class);
        // setDelegate Test
        ((DelegatingWriteAccess)delegatingLocalDateTimeWriteAccess).setDelegateAccess(localDateTimeWriteAccess);
        assertThat(delegatingLocalDateTimeWriteAccess.toString()).isNotNull(); // This asserts that the access is set. If it is not set, we would get a NPE here.

        // setFrom Test
        delegatingLocalDateTimeWriteAccess.setFrom(localDateTimeReadAccess);
        verify(localDateTimeWriteAccess).setFrom(localDateTimeReadAccess);

        // setMissing Test
        delegatingLocalDateTimeWriteAccess.setMissing();
        verify(localDateTimeWriteAccess).setMissing();

        // setLocalDateTimeValue Test
        delegatingLocalDateTimeWriteAccess.setLocalDateTimeValue(LocalDateTime.of(2021, 10, 18, 11, 53));
        verify(localDateTimeWriteAccess).setLocalDateTimeValue(LocalDateTime.of(2021, 10, 18, 11, 53));
    }


    @Test
    public void testLocalTime() {
        // Constructor Test
        var delegatingLocalTimeWriteAccess = (LocalTimeWriteAccess)DelegatingWriteAccesses.createDelegatingWriteAccess(DataSpec.localTimeSpec());
        // Mock Setup
        var localTimeWriteAccess = mock(LocalTimeWriteAccess.class);
        var localTimeReadAccess = mock(LocalTimeReadAccess.class);
        // setDelegate Test
        ((DelegatingWriteAccess)delegatingLocalTimeWriteAccess).setDelegateAccess(localTimeWriteAccess);
        assertThat(delegatingLocalTimeWriteAccess.toString()).isNotNull(); // This asserts that the access is set. If it is not set, we would get a NPE here.

        // setFrom Test
        delegatingLocalTimeWriteAccess.setFrom(localTimeReadAccess);
        verify(localTimeWriteAccess).setFrom(localTimeReadAccess);

        // setMissing Test
        delegatingLocalTimeWriteAccess.setMissing();
        verify(localTimeWriteAccess).setMissing();

        // setLocalTimeValue Test
        delegatingLocalTimeWriteAccess.setLocalTimeValue(LocalTime.of(12, 24));
        verify(localTimeWriteAccess).setLocalTimeValue(LocalTime.of(12, 24));
    }


    @Test
    public void testPeriod() {
        // Constructor Test
        var delegatingPeriodWriteAccess = (PeriodWriteAccess)DelegatingWriteAccesses.createDelegatingWriteAccess(DataSpec.periodSpec());
        // Mock Setup
        var periodWriteAccess = mock(PeriodWriteAccess.class);
        var periodReadAccess = mock(PeriodReadAccess.class);
        // setDelegate Test
        ((DelegatingWriteAccess)delegatingPeriodWriteAccess).setDelegateAccess(periodWriteAccess);
        assertThat(delegatingPeriodWriteAccess.toString()).isNotNull(); // This asserts that the access is set. If it is not set, we would get a NPE here.

        // setFrom Test
        delegatingPeriodWriteAccess.setFrom(periodReadAccess);
        verify(periodWriteAccess).setFrom(periodReadAccess);

        // setMissing Test
        delegatingPeriodWriteAccess.setMissing();
        verify(periodWriteAccess).setMissing();

        // setPeriodValue Test
        delegatingPeriodWriteAccess.setPeriodValue(Period.of(2, 1, 0));
        verify(periodWriteAccess).setPeriodValue(Period.of(2, 1, 0));
    }


    @SuppressWarnings("unchecked")
    @Test
    public void testVarBinary() {
        // Constructor Test
        var delegatingVarBinaryWriteAccess = (VarBinaryWriteAccess)DelegatingWriteAccesses.createDelegatingWriteAccess(DataSpec.varBinarySpec());
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


    @Test
    public void testZonedDateTime() {
        // Constructor Test
        var delegatingZonedDateTimeWriteAccess = (ZonedDateTimeWriteAccess)DelegatingWriteAccesses.createDelegatingWriteAccess(DataSpec.zonedDateTimeSpec());
        // Mock Setup
        var zonedDateTimeWriteAccess = mock(ZonedDateTimeWriteAccess.class);
        var zonedDateTimeReadAccess = mock(ZonedDateTimeReadAccess.class);
        // setDelegate Test
        ((DelegatingWriteAccess)delegatingZonedDateTimeWriteAccess).setDelegateAccess(zonedDateTimeWriteAccess);
        assertThat(delegatingZonedDateTimeWriteAccess.toString()).isNotNull(); // This asserts that the access is set. If it is not set, we would get a NPE here.

        // setFrom Test
        delegatingZonedDateTimeWriteAccess.setFrom(zonedDateTimeReadAccess);
        verify(zonedDateTimeWriteAccess).setFrom(zonedDateTimeReadAccess);

        // setMissing Test
        delegatingZonedDateTimeWriteAccess.setMissing();
        verify(zonedDateTimeWriteAccess).setMissing();

        // setZonedDateTimeValue Test
        var localDateTime = LocalDateTime.of(2021, 10, 18, 11, 53);
        var zonedDateTime = ZonedDateTime.of(localDateTime, ZoneId.of("Europe/Paris"));
        delegatingZonedDateTimeWriteAccess.setZonedDateTimeValue(zonedDateTime);
        verify(zonedDateTimeWriteAccess).setZonedDateTimeValue(zonedDateTime);
    }



}
