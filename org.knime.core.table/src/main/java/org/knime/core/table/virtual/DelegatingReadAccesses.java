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
 *   Created on May 11, 2021 by marcel
 */
package org.knime.core.table.virtual;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZonedDateTime;
import java.util.Arrays;

import org.knime.core.table.access.BooleanAccess.BooleanReadAccess;
import org.knime.core.table.access.ByteAccess.ByteReadAccess;
import org.knime.core.table.access.DoubleAccess.DoubleReadAccess;
import org.knime.core.table.access.DurationAccess.DurationReadAccess;
import org.knime.core.table.access.FloatAccess.FloatReadAccess;
import org.knime.core.table.access.IntAccess.IntReadAccess;
import org.knime.core.table.access.ListAccess.ListReadAccess;
import org.knime.core.table.access.LocalDateAccess.LocalDateReadAccess;
import org.knime.core.table.access.LocalDateTimeAccess.LocalDateTimeReadAccess;
import org.knime.core.table.access.LocalTimeAccess.LocalTimeReadAccess;
import org.knime.core.table.access.LongAccess.LongReadAccess;
import org.knime.core.table.access.PeriodAccess.PeriodReadAccess;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.access.StringAccess.StringReadAccess;
import org.knime.core.table.access.StructAccess.StructReadAccess;
import org.knime.core.table.access.VarBinaryAccess.VarBinaryReadAccess;
import org.knime.core.table.access.ZonedDateTimeAccess.ZonedDateTimeReadAccess;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.schema.BooleanDataSpec;
import org.knime.core.table.schema.ByteDataSpec;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.DoubleDataSpec;
import org.knime.core.table.schema.DurationDataSpec;
import org.knime.core.table.schema.FloatDataSpec;
import org.knime.core.table.schema.IntDataSpec;
import org.knime.core.table.schema.ListDataSpec;
import org.knime.core.table.schema.LocalDateDataSpec;
import org.knime.core.table.schema.LocalDateTimeDataSpec;
import org.knime.core.table.schema.LocalTimeDataSpec;
import org.knime.core.table.schema.LongDataSpec;
import org.knime.core.table.schema.PeriodDataSpec;
import org.knime.core.table.schema.StringDataSpec;
import org.knime.core.table.schema.StructDataSpec;
import org.knime.core.table.schema.VarBinaryDataSpec;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectDeserializer;
import org.knime.core.table.schema.VoidDataSpec;
import org.knime.core.table.schema.ZonedDateTimeDataSpec;

/**
 * Provides implementations of {@link ReadAccess ReadAccesses} that simply delegate to another {@link ReadAccess}.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class DelegatingReadAccesses {

    private DelegatingReadAccesses() {
    }

    /**
     * Creates a {@link ReadAccess} for the provided type that delegates to another {@link ReadAccess} of the same type.
     *
     * @param spec the type of ReadAccess
     * @return a {@link DelegatingReadAccess} with the provided {@link DataSpec}
     */
    public static DelegatingReadAccess<?> createDelegatingAccess(final DataSpec spec) {
        return spec.accept(DataSpecToDelegatingReadAccessMapper.INSTANCE);
    }

    /**
     * Creates a {@link ReadAccessRow} that delegates to another ReadAccessRow with the same schema.
     *
     * @param schema of the ReadAccessRow
     * @return a {@link DelegatingReadAccessRow} with the provided schema
     */
    public static DelegatingReadAccessRow createDelegatingReadAccessRow(final ColumnarSchema schema) {
        return new DefaultDelegatingReadAccessRow(schema);
    }

    private static class DefaultDelegatingReadAccessRow implements DelegatingReadAccessRow {

        private final DelegatingReadAccess<?>[] m_accesses;

        private final ColumnarSchema m_schema;

        DefaultDelegatingReadAccessRow(final ColumnarSchema schema) {
            m_schema = schema;
            m_accesses = new DelegatingReadAccess[m_schema.numColumns()];
            for (int i = 0; i < m_accesses.length; i++) {
                m_accesses[i] = DelegatingReadAccesses.createDelegatingAccess(m_schema.getSpec(i));
            }
        }

        @Override
        public void setDelegateAccess(final ReadAccessRow delegateAccess) {
            for (int i = 0; i < m_schema.numColumns(); i++) {
                m_accesses[i].setDelegateAccess(delegateAccess.getAccess(i));
            }
        }

        @Override
        public int size() {
            return m_schema.numColumns();
        }

        @Override
        public <A extends ReadAccess> A getAccess(final int index) {
            @SuppressWarnings("unchecked")
            final A casted = (A)m_accesses[index];
            return casted;
        }

        @Override
        public String toString() {
            return Arrays.toString(m_accesses);
        }

    }

    /**
     * A {@link ReadAccessRow} that delegates to another {@link ReadAccessRow} with the same schema. The other
     * {@link ReadAccessRow} can be changed using the {@link #setDelegateAccess(ReadAccessRow)} method.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    public static interface DelegatingReadAccessRow extends ReadAccessRow {

        /**
         * Sets the access that this instance delegates to.
         *
         * @param access to delegate to
         */
        void setDelegateAccess(final ReadAccessRow access);
    }

    /**
     * A {@link ReadAccess} that delegates to another {@link ReadAccess} of the same type.
     *
     * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     * @param <A> the type of {@link ReadAccess} to delegate to
     */
    public static interface DelegatingReadAccess<A extends ReadAccess> extends ReadAccess {

        /**
         * Sets the access this access delegates to.
         *
         * @param access to delegate to
         */
        void setDelegateAccess(A access);
    }

    private static final class DataSpecToDelegatingReadAccessMapper
        implements DataSpec.Mapper<DelegatingReadAccess<?>> {

        private static final DataSpecToDelegatingReadAccessMapper INSTANCE = new DataSpecToDelegatingReadAccessMapper();

        @Override
        public DelegatingReadAccess<?> visit(final BooleanDataSpec spec) {
            return new DelegatingBooleanReadAccess();
        }

        @Override
        public DelegatingReadAccess<?> visit(final ByteDataSpec spec) {
            return new DelegatingByteReadAccess();
        }

        @Override
        public DelegatingReadAccess<?> visit(final DoubleDataSpec spec) {
            return new DelegatingDoubleReadAccess();
        }

        @Override
        public DelegatingReadAccess<?> visit(final DurationDataSpec spec) {
            return new DelegatingDurationReadAccess();
        }

        @Override
        public DelegatingReadAccess<?> visit(final FloatDataSpec spec) {
            return new DelegatingFloatReadAccess();
        }

        @Override
        public DelegatingReadAccess<?> visit(final IntDataSpec spec) {
            return new DelegatingIntReadAccess();
        }

        @Override
        public DelegatingReadAccess<?> visit(final LocalDateDataSpec spec) {
            return new DelegatingLocalDateReadAccess();
        }

        @Override
        public DelegatingReadAccess<?> visit(final LocalDateTimeDataSpec spec) {
            return new DelegatingLocalDateTimeReadAccess();
        }

        @Override
        public DelegatingReadAccess<?> visit(final LocalTimeDataSpec spec) {
            return new DelegatingLocalTimeReadAccess();
        }

        @Override
        public DelegatingReadAccess<?> visit(final LongDataSpec spec) {
            return new DelegatingLongReadAccess();
        }

        @Override
        public DelegatingReadAccess<?> visit(final PeriodDataSpec spec) {
            return new DelegatingPeriodReadAccess();
        }

        @Override
        public DelegatingReadAccess<?> visit(final VarBinaryDataSpec spec) {
            return new DelegatingVarBinaryReadAccess();
        }

        @Override
        public DelegatingReadAccess<?> visit(final VoidDataSpec spec) {
            return new AbstractDelegatingReadAccess<ReadAccess>() {
            };
        }

        @Override
        public DelegatingReadAccess<?> visit(final StructDataSpec spec) {
            return new DelegatingStructReadAccess();
        }

        @Override
        public DelegatingReadAccess<?> visit(final ListDataSpec listDataSpec) {
            return new DelegatingListReadAccess();
        }

        @Override
        public DelegatingReadAccess<?> visit(final ZonedDateTimeDataSpec spec) {
            return new DelegatingZonedDateTimeReadAccess();
        }

        @Override
        public DelegatingReadAccess<?> visit(final StringDataSpec spec) {
            return new DelegatingStringReadAccess();
        }
    }

    private static final class DelegatingBooleanReadAccess extends AbstractDelegatingReadAccess<BooleanReadAccess>
        implements BooleanReadAccess {

        @Override
        public boolean getBooleanValue() {
            return m_delegateAccess.getBooleanValue();
        }
    }

    private static final class DelegatingByteReadAccess extends AbstractDelegatingReadAccess<ByteReadAccess>
        implements ByteReadAccess {

        @Override
        public byte getByteValue() {
            return m_delegateAccess.getByteValue();
        }
    }

    private static final class DelegatingDoubleReadAccess extends AbstractDelegatingReadAccess<DoubleReadAccess>
        implements DoubleReadAccess {

        @Override
        public double getDoubleValue() {
            return m_delegateAccess.getDoubleValue();
        }
    }

    private static final class DelegatingFloatReadAccess extends AbstractDelegatingReadAccess<FloatReadAccess>
        implements FloatReadAccess {

        @Override
        public float getFloatValue() {
            return m_delegateAccess.getFloatValue();
        }
    }

    private static final class DelegatingIntReadAccess extends AbstractDelegatingReadAccess<IntReadAccess>
        implements IntReadAccess {

        @Override
        public int getIntValue() {
            return m_delegateAccess.getIntValue();
        }
    }

    private static final class DelegatingListReadAccess extends AbstractDelegatingReadAccess<ListReadAccess>
        implements ListReadAccess {

        @Override
        public int size() {
            return m_delegateAccess.size();
        }

        @Override
        public <R extends ReadAccess> R getAccess(final int index) {
            return m_delegateAccess.getAccess(index);
        }

        @Override
        public boolean isMissing(final int index) {
            return m_delegateAccess.isMissing(index);
        }
    }

    private static final class DelegatingLongReadAccess extends AbstractDelegatingReadAccess<LongReadAccess>
        implements LongReadAccess {

        @Override
        public long getLongValue() {
            return m_delegateAccess.getLongValue();
        }
    }

    private static final class DelegatingStringReadAccess extends AbstractDelegatingReadAccess<StringReadAccess>
        implements StringReadAccess {

        @Override
        public String getStringValue() {
            return m_delegateAccess.getStringValue();
        }
    }

    private static final class DelegatingStructReadAccess extends AbstractDelegatingReadAccess<StructReadAccess>
        implements StructReadAccess {

        @Override
        public int size() {
            return m_delegateAccess.size();
        }

        @Override
        public <R extends ReadAccess> R getAccess(final int index) {
            return m_delegateAccess.getAccess(index);
        }
    }

    private static final class DelegatingVarBinaryReadAccess extends AbstractDelegatingReadAccess<VarBinaryReadAccess>
        implements VarBinaryReadAccess {

        @Override
        public byte[] getByteArray() {
            return m_delegateAccess.getByteArray();
        }

        @Override
        public <T> T getObject(final ObjectDeserializer<T> deserializer) {
            return m_delegateAccess.getObject(deserializer);
        }
    }

    private static final class DelegatingDurationReadAccess extends AbstractDelegatingReadAccess<DurationReadAccess>
        implements DurationReadAccess {

        @Override
        public Duration getDurationValue() {
            return m_delegateAccess.getDurationValue();
        }

    }

    private static final class DelegatingPeriodReadAccess extends AbstractDelegatingReadAccess<PeriodReadAccess>
        implements PeriodReadAccess {

        @Override
        public Period getPeriodValue() {
            return m_delegateAccess.getPeriodValue();
        }

    }

    private static final class DelegatingZonedDateTimeReadAccess
        extends AbstractDelegatingReadAccess<ZonedDateTimeReadAccess> implements ZonedDateTimeReadAccess {

        @Override
        public ZonedDateTime getZonedDateTimeValue() {
            return m_delegateAccess.getZonedDateTimeValue();
        }

    }

    private static final class DelegatingLocalTimeReadAccess extends AbstractDelegatingReadAccess<LocalTimeReadAccess>
        implements LocalTimeReadAccess {

        @Override
        public LocalTime getLocalTimeValue() {
            return m_delegateAccess.getLocalTimeValue();
        }

    }

    private static final class DelegatingLocalDateTimeReadAccess
        extends AbstractDelegatingReadAccess<LocalDateTimeReadAccess> implements LocalDateTimeReadAccess {

        @Override
        public LocalDateTime getLocalDateTimeValue() {
            return m_delegateAccess.getLocalDateTimeValue();
        }

    }

    private static final class DelegatingLocalDateReadAccess extends AbstractDelegatingReadAccess<LocalDateReadAccess>
        implements LocalDateReadAccess {

        @Override
        public LocalDate getLocalDateValue() {
            return m_delegateAccess.getLocalDateValue();
        }

    }

    private abstract static class AbstractDelegatingReadAccess<A extends ReadAccess>
        implements DelegatingReadAccess<A> {

        protected A m_delegateAccess;

        @Override
        public void setDelegateAccess(final A access) {
            m_delegateAccess = access;
        }

        @Override
        public boolean isMissing() {
            return m_delegateAccess.isMissing();
        }

        @Override
        public DataSpec getDataSpec() {
            return m_delegateAccess.getDataSpec();
        }

        @Override
        public String toString() {
            return m_delegateAccess.toString();
        }
    }
}
