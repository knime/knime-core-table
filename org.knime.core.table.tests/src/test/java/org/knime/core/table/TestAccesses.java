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
 *   Oct 22, 2020 (dietzc): created
 */
package org.knime.core.table;

import java.time.Duration;

import org.knime.core.table.access.BooleanAccess.BooleanReadAccess;
import org.knime.core.table.access.BooleanAccess.BooleanWriteAccess;
import org.knime.core.table.access.ByteAccess.ByteReadAccess;
import org.knime.core.table.access.ByteAccess.ByteWriteAccess;
import org.knime.core.table.access.DoubleAccess.DoubleReadAccess;
import org.knime.core.table.access.DoubleAccess.DoubleWriteAccess;
import org.knime.core.table.access.DurationAccess.DurationReadAccess;
import org.knime.core.table.access.DurationAccess.DurationWriteAccess;
import org.knime.core.table.access.FloatAccess.FloatReadAccess;
import org.knime.core.table.access.FloatAccess.FloatWriteAccess;
import org.knime.core.table.access.IntAccess.IntReadAccess;
import org.knime.core.table.access.IntAccess.IntWriteAccess;
import org.knime.core.table.access.LongAccess.LongReadAccess;
import org.knime.core.table.access.LongAccess.LongWriteAccess;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.access.StringAccess.StringReadAccess;
import org.knime.core.table.access.StringAccess.StringWriteAccess;
import org.knime.core.table.access.VarBinaryAccess.VarBinaryReadAccess;
import org.knime.core.table.access.VarBinaryAccess.VarBinaryWriteAccess;
import org.knime.core.table.access.WriteAccess;
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
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectSerializer;
import org.knime.core.table.schema.VoidDataSpec;
import org.knime.core.table.schema.ZonedDateTimeDataSpec;

/**
 * A collection of Test access implementations that can be retrieved by mapping from a given {@link DataSpec}.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @since 4.4
 */
public final class TestAccesses {

    private TestAccesses() {}

    /**
     * Create {@link TestAccess} for the provided schema. Each access is backed by an object array of length length
     *
     * @param schema to derive the accesses from
     * @return the mapped {@link TestAccess} array
     */
    public static TestAccess[] createTestAccesses(final ColumnarSchema schema) {
        final TestAccess[] accesses = new TestAccess[schema.numColumns()];
        for (int i = 0; i < schema.numColumns(); i++) {
            accesses[i] = createTestAccess(schema.getSpec(i));
        }
        return accesses;
    }

    /**
     * @param spec to create TestAccess from
     * @return the TestAccess
     */
    public static TestAccess createTestAccess(final DataSpec spec) {
        return spec.accept(DataSpecToTestAccessMapper.INSTANCE);
    }

    /**
     * Simple marker interface for combined ReadAccess and WriteAccess
     *
     * @author Marc Bux, KNIME GmbH, Berlin, Germany
     * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
     */
    public interface TestAccess extends ReadAccess, WriteAccess {
        /**
         * @param index the index in the object array the access is currently pointing to
         */
        void setIndex(int index);
    }

    private static final class DataSpecToTestAccessMapper implements DataSpec.Mapper<TestAccess> {

        private static final DataSpecToTestAccessMapper INSTANCE = new DataSpecToTestAccessMapper();

        private DataSpecToTestAccessMapper() {}

        @Override
        public TestAccess visit(final StructDataSpec spec) {
            throw new UnsupportedOperationException("nyi");
        }

        @Override
        public TestAccess visit(final DoubleDataSpec spec) {
            return new TestDoubleAccess();
        }

        @Override
        public TestAccess visit(final BooleanDataSpec spec) {
            return new TestBooleanAccess();
        }

        @Override
        public TestAccess visit(final IntDataSpec spec) {
            return new TestIntAccess();
        }

        @Override
        public TestAccess visit(final LongDataSpec spec) {
            return new TestLongAccess();
        }

        @Override
        public TestAccess visit(final VoidDataSpec spec) {
            return TestVoidAccess.INSTANCE;
        }

        @Override
        public TestAccess visit(final VarBinaryDataSpec spec) {
            return new TestVarBinaryAccess();
        }

        @Override
        public TestAccess visit(final DurationDataSpec spec) {
            return new TestDurationAccess();
        }

        @Override
        public TestAccess visit(final StringDataSpec spec) {
            return new TestStringAccess();
        }

        @Override
        public TestAccess visit(final ByteDataSpec spec) {
            return new TestByteAccess();
        }

        @Override
        public TestAccess visit(final FloatDataSpec spec) {
            return new TestFloatAccess();
        }

        @Override
        public TestAccess visit(final ListDataSpec spec) {
            throw new UnsupportedOperationException("nyi");
        }

        @Override
        public TestAccess visit(final LocalDateDataSpec spec) {
            throw new UnsupportedOperationException("nyi");
        }

        @Override
        public TestAccess visit(final LocalTimeDataSpec spec) {
            throw new UnsupportedOperationException("nyi");
        }

        @Override
        public TestAccess visit(final LocalDateTimeDataSpec spec) {
            throw new UnsupportedOperationException("nyi");
        }

        @Override
        public TestAccess visit(final PeriodDataSpec spec) {
            throw new UnsupportedOperationException("nyi");
        }

        @Override
        public TestAccess visit(final ZonedDateTimeDataSpec spec) {
            throw new UnsupportedOperationException("nyi");
        }

        private static final class TestBooleanAccess extends AbstractTestAccess<Boolean>
            implements BooleanReadAccess, BooleanWriteAccess {

            @Override
            public boolean getBooleanValue() {
                return get();
            }

            @Override
            public void setBooleanValue(final boolean value) {
                set(value);
            }

            @Override
            protected void setFromNonMissing(final ReadAccess access) {
                set(((BooleanReadAccess)access).getBooleanValue());
            }

        }

        private static final class TestByteAccess extends AbstractTestAccess<Byte>
            implements ByteReadAccess, ByteWriteAccess {

            @Override
            public void setByteValue(final byte value) {
                set(value);
            }

            @Override
            public byte getByteValue() {
                return get();
            }

            @Override
            protected void setFromNonMissing(final ReadAccess access) {
                set(((ByteReadAccess)access).getByteValue());
            }

        }

        private static final class TestDoubleAccess extends AbstractTestAccess<Double>
            implements DoubleWriteAccess, DoubleReadAccess {

            @Override
            public void setDoubleValue(final double value) {
                set(value);
            }

            @Override
            public double getDoubleValue() {
                return get();
            }

            @Override
            protected void setFromNonMissing(final ReadAccess access) {
                set(((DoubleReadAccess)access).getDoubleValue());
            }

        }

        private static final class TestDurationAccess extends AbstractTestAccess<Duration>
            implements DurationReadAccess, DurationWriteAccess {

            @Override
            public void setDurationValue(final Duration duration) {
                set(duration);
            }

            @Override
            public Duration getDurationValue() {
                return get();
            }

            @Override
            protected void setFromNonMissing(final ReadAccess access) {
                set(((DurationReadAccess)access).getDurationValue());
            }

        }

        private static final class TestFloatAccess extends AbstractTestAccess<Float>
            implements FloatReadAccess, FloatWriteAccess {

            @Override
            public void setFloatValue(final float value) {
                set(value);
            }

            @Override
            public float getFloatValue() {
                return get();
            }

            @Override
            protected void setFromNonMissing(final ReadAccess access) {
                set(((FloatReadAccess)access).getFloatValue());
            }

        }

        private static final class TestIntAccess extends AbstractTestAccess<Integer>
            implements IntReadAccess, IntWriteAccess {

            @Override
            public int getIntValue() {
                return get();
            }

            @Override
            public void setIntValue(final int value) {
                set(value);
            }

            @Override
            protected void setFromNonMissing(final ReadAccess access) {
                set(((IntReadAccess)access).getIntValue());
            }

        }

        private static final class TestLongAccess extends AbstractTestAccess<Long>
            implements LongReadAccess, LongWriteAccess {

            @Override
            public long getLongValue() {
                return get();
            }

            @Override
            public void setLongValue(final long value) {
                set(value);
            }

            @Override
            protected void setFromNonMissing(final ReadAccess access) {
                set(((LongReadAccess)access).getLongValue());
            }

        }

        private static final class TestStringAccess extends AbstractTestAccess<String>
            implements StringReadAccess, StringWriteAccess {

            @Override
            public void setStringValue(final String value) {
                set(value);
            }

            @Override
            public String getStringValue() {
                return get();
            }

            @Override
            protected void setFromNonMissing(final ReadAccess access) {
                set(((StringReadAccess)access).getStringValue());
            }

        }

        private static final class TestVarBinaryAccess extends AbstractTestAccess<byte[]>
            implements VarBinaryReadAccess, VarBinaryWriteAccess {

            @Override
            public void setByteArray(final byte[] value) {
                set(value);
            }

            @Override
            public void setByteArray(final byte[] array, final int index, final int length) {
                byte[] value = new byte[length];
                System.arraycopy(array, index, value, 0, length);
                set(value);
            }

            @Override
            public byte[] getByteArray() {
                return get();
            }

            @Override
            public <T> void setObject(final T value, final ObjectSerializer<T> serializer) {
                throw new UnsupportedOperationException("nyi");
            }

            @Override
            public <T> T getObject(final ObjectDeserializer<T> deserializer) {
                throw new UnsupportedOperationException("nyi");
            }

            @Override
            protected void setFromNonMissing(final ReadAccess access) {
                set(((VarBinaryReadAccess)access).getByteArray());
            }

        }

        private static final class TestVoidAccess implements TestAccess {

            @SuppressWarnings("hiding")
            private static final TestVoidAccess INSTANCE = new TestVoidAccess();

            private TestVoidAccess() {}

            @Override
            public DataSpec getDataSpec() {
                return DataSpec.voidSpec();
            }

            @Override
            public boolean isMissing() {
                return true;
            }

            @Override
            public void setMissing() {
                // not to be called
            }

            @Override
            public void setFrom(final ReadAccess access) {
                // not to be called
            }

            @Override
            public void setIndex(final int index) {
                // not to be called
            }
        }

        private abstract static class AbstractTestAccess<T> implements TestAccess {

            private Object[] m_data;

            private int m_index = 0;

            public AbstractTestAccess() {
                m_data = new Object[16];
            }

            @Override
            public final boolean isMissing() {
                return m_data[m_index] == null;
            }

            private void ensureSize() {
                if (m_data.length <= m_index) {
                    final Object[] data = new Object[m_data.length * 2];
                    System.arraycopy(m_data, 0, data, 0, m_data.length);
                    m_data = data;
                }
            }

            @Override
            public final void setMissing() {
                m_data[m_index] = null;
            }

            @Override
            public void setFrom(final ReadAccess access) {
                if (access.isMissing()) {
                    m_data[m_index] = null;
                } else {
                    setFromNonMissing(access);
                }
            }

            @Override
            public void setIndex(final int index) {
                ensureSize();
                m_index = index;
            }

            protected void set(final T data) {
                m_data[m_index] = data;
            }

            protected T get() {
                @SuppressWarnings("unchecked")
                final T cast = (T)m_data[m_index];
                return cast;
            }

            protected abstract void setFromNonMissing(ReadAccess access);
        }
    }
}