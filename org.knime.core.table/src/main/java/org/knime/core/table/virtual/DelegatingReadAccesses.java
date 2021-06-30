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

import org.knime.core.table.access.BooleanAccess.BooleanReadAccess;
import org.knime.core.table.access.ByteAccess.ByteReadAccess;
import org.knime.core.table.access.DoubleAccess.DoubleReadAccess;
import org.knime.core.table.access.FloatAccess.FloatReadAccess;
import org.knime.core.table.access.IntAccess.IntReadAccess;
import org.knime.core.table.access.ListAccess.ListReadAccess;
import org.knime.core.table.access.LongAccess.LongReadAccess;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.access.StringAccess.StringReadAccess;
import org.knime.core.table.access.StructAccess.StructReadAccess;
import org.knime.core.table.access.VarBinaryAccess.VarBinaryReadAccess;
import org.knime.core.table.schema.BooleanDataSpec;
import org.knime.core.table.schema.ByteDataSpec;
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
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
final class DelegatingReadAccesses {

    private DelegatingReadAccesses() {}

    public static DelegatingReadAccess<?> createDelegatingAccess(final DataSpec spec) {
        return spec.accept(DataSpecToDelegatingReadAccessMapper.INSTANCE);
    }

    public static interface DelegatingReadAccess<A extends ReadAccess> extends ReadAccess {

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
            throw new IllegalStateException("not yet implemented"); // TODO: implement
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
            throw new IllegalStateException("not yet implemented"); // TODO: implement
        }

        @Override
        public DelegatingReadAccess<?> visit(final LocalDateTimeDataSpec spec) {
            throw new IllegalStateException("not yet implemented"); // TODO: implement
        }

        @Override
        public DelegatingReadAccess<?> visit(final LocalTimeDataSpec spec) {
            throw new IllegalStateException("not yet implemented"); // TODO: implement
        }

        @Override
        public DelegatingReadAccess<?> visit(final LongDataSpec spec) {
            return new DelegatingLongReadAccess();
        }

        @Override
        public DelegatingReadAccess<?> visit(final PeriodDataSpec spec) {
            throw new IllegalStateException("not yet implemented"); // TODO: implement
        }

        @Override
        public DelegatingReadAccess<?> visit(final VarBinaryDataSpec spec) {
            return new DelegatingVarBinaryReadAccess();
        }

        @Override
        public DelegatingReadAccess<?> visit(final VoidDataSpec spec) {
            return new AbstractDelegatingReadAccess<ReadAccess>() {};
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
            throw new IllegalStateException("not yet implemented"); // TODO: implement
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
    }
}
