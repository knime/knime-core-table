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
package org.knime.core.table.access;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZonedDateTime;
import java.util.Arrays;

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
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.WriteAccessRow;
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
 * A collection of buffered access implementations that can be retrieved by mapping from a given {@link DataSpec}.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @since 4.3
 */
public final class BufferedAccesses {

    private BufferedAccesses() {
    }

    /**
     * Creates a {@link BufferedAccess} for the provided {@link DataSpec}.
     *
     * @param spec for which a {@link BufferedAccess} is required
     * @return a {@link BufferedAccess} for the provided {@link DataSpec}
     */
    public static BufferedAccess createBufferedAccess(final DataSpec spec) {
        return spec.accept(DataSpecToBufferedAccessMapper.INSTANCE);
    }

    /**
     * Creates a {@link BufferedAccessRow} with the provided {@link ColumnarSchema}.
     *
     * @param schema defining the number of columns and their types
     * @return a {@link BufferedAccessRow} with the provided {@link ColumnarSchema}
     */
    public static BufferedAccessRow createBufferedAccessRow(final ColumnarSchema schema) {
        return new DefaultBufferedAccessRow(schema);
    }

    /**
     * Convenience method for creating an array of BufferedAccesses for a provided schema.
     *
     * @param schema for which to create the BufferedAccesses
     * @return BufferedAccesses corresponding to the DataSpecs in the provided schema
     */
    private static BufferedAccess[] createBufferedAccesses(final ColumnarSchema schema) {
        return schema.specStream()//
            .map(BufferedAccesses::createBufferedAccess)//
            .toArray(BufferedAccess[]::new);
    }

    /**
     * Both a {@link ReadAccessRow} and {@link WriteAccessRow} that is based on {@link BufferedAccess BufferedAccesses}.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    public interface BufferedAccessRow extends ReadAccessRow, WriteAccessRow {

        /**
         * Provides access to values within the individual columns.<br>
         * <b>NOTE:</b> Repetitive calls with the same index return the same access instance but the instance may point
         * to different values. It is the responsibility of the caller to retrieve the value from the access if they
         * want to store it.
         *
         * @param <A> the concrete type of access
         * @param index to the column for which to get the access
         * @return the access for column
         */
        public <A extends BufferedAccess> A getBufferedAccess(int index);
    }

    private static final class DefaultBufferedAccessRow implements BufferedAccessRow {

        private final BufferedAccess[] m_accesses;

        private DefaultBufferedAccessRow(final ColumnarSchema schema) {
            m_accesses = createBufferedAccesses(schema);
        }

        @Override
        public int size() {
            return m_accesses.length;
        }

        @Override
        public <A extends ReadAccess> A getAccess(final int index) {
            return getBufferedAccess(index);
        }

        @Override
        public void setFrom(final ReadAccessRow readAccessRow) {
            if (readAccessRow.size() != size()) {
                throw new IllegalArgumentException(
                    String.format("Wrong size: %d vs. %d", readAccessRow.size(), size()));
            }
            for (var i = 0; i < size(); i++) {
                m_accesses[i].setFrom(readAccessRow.getAccess(i));
            }
        }

        @Override
        public <A extends WriteAccess> A getWriteAccess(final int index) {
            return getBufferedAccess(index);
        }

        @SuppressWarnings("unchecked")
        @Override
        public <A extends BufferedAccess> A getBufferedAccess(final int index) {
            return (A)m_accesses[index];
        }

    }

    /**
     * Simple marker interface for combined ReadAccess and WriteAccess
     *
     * @author Marc Bux, KNIME GmbH, Berlin, Germany
     * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
     */
    public interface BufferedAccess extends ReadAccess, WriteAccess {
    }

    private static final class DataSpecToBufferedAccessMapper implements DataSpec.Mapper<BufferedAccess> {

        private static final DataSpecToBufferedAccessMapper INSTANCE = new DataSpecToBufferedAccessMapper();

        @Override
        public BufferedAccess visit(final StructDataSpec spec) {
            return new BufferedStructAccess(spec);
        }

        @Override
        public BufferedAccess visit(final DoubleDataSpec spec) {
            return new BufferedDoubleAccess();
        }

        @Override
        public BufferedAccess visit(final BooleanDataSpec spec) {
            return new BufferedBooleanAccess();
        }

        @Override
        public BufferedAccess visit(final IntDataSpec spec) {
            return new BufferedIntAccess();
        }

        @Override
        public BufferedAccess visit(final LongDataSpec spec) {
            return new BufferedLongAccess();
        }

        @Override
        public BufferedAccess visit(final VoidDataSpec spec) {
            return BufferedVoidAccess.VOID_ACCESS_INSTANCE;
        }

        @Override
        public BufferedAccess visit(final VarBinaryDataSpec spec) {
            return new BufferedVarBinaryAccess();
        }

        @Override
        public BufferedAccess visit(final ListDataSpec spec) {
            return new BufferedListAccess(spec);
        }

        @Override
        public BufferedAccess visit(final LocalDateDataSpec spec) {
            return new BufferedLocalDateAccess();
        }

        @Override
        public BufferedAccess visit(final LocalTimeDataSpec spec) {
            return new BufferedLocalTimeAccess();
        }

        @Override
        public BufferedAccess visit(final LocalDateTimeDataSpec spec) {
            return new BufferedLocalDateTimeAccess();
        }

        @Override
        public BufferedAccess visit(final DurationDataSpec spec) {
            return new BufferedDurationAccess();
        }

        @Override
        public BufferedAccess visit(final PeriodDataSpec spec) {
            return new BufferedPeriodAccess();
        }

        @Override
        public BufferedAccess visit(final ZonedDateTimeDataSpec spec) {
            return new BufferedZonedDateTimeAccess();
        }

        @Override
        public BufferedAccess visit(final StringDataSpec spec) {
            return new BufferedStringAccess();
        }

        @Override
        public BufferedAccess visit(final ByteDataSpec spec) {
            return new BufferedByteAccess();
        }

        @Override
        public BufferedAccess visit(final FloatDataSpec spec) {
            return new BufferedFloatAccess();
        }

        private static final class BufferedBooleanAccess extends AbstractBufferedAccess
            implements BooleanReadAccess, BooleanWriteAccess {

            private boolean m_value;

            @Override
            public boolean getBooleanValue() {
                return m_value;
            }

            @Override
            public void setBooleanValue(final boolean value) {
                m_value = value;
                m_isMissing = false;
            }

            @Override
            protected void setFromNonMissing(final ReadAccess access) {
                m_value = ((BooleanReadAccess)access).getBooleanValue();
            }

        }

        private static final class BufferedByteAccess extends AbstractBufferedAccess
            implements ByteReadAccess, ByteWriteAccess {

            private byte m_value;

            @Override
            public void setByteValue(final byte value) {
                m_value = value;
                m_isMissing = false;
            }

            @Override
            public byte getByteValue() {
                return m_value;
            }

            @Override
            protected void setFromNonMissing(final ReadAccess access) {
                m_value = ((ByteReadAccess)access).getByteValue();
            }

        }

        private static final class BufferedDoubleAccess extends AbstractBufferedAccess
            implements DoubleWriteAccess, DoubleReadAccess {

            private double m_value;

            @Override
            public void setDoubleValue(final double value) {
                m_value = value;
                m_isMissing = false;
            }

            @Override
            public double getDoubleValue() {
                return m_value;
            }

            @Override
            protected void setFromNonMissing(final ReadAccess access) {
                m_value = ((DoubleReadAccess)access).getDoubleValue();
            }

        }

        private static final class BufferedDurationAccess extends AbstractBufferedObjectAccess<Duration>
            implements DurationReadAccess, DurationWriteAccess {

            @Override
            public void setDurationValue(final Duration value) {
                m_value = value;
            }

            @Override
            public Duration getDurationValue() {
                return m_value;
            }

            @Override
            protected void setFromNonMissing(final ReadAccess access) {
                m_value = ((DurationReadAccess)access).getDurationValue();
            }

        }

        private static final class BufferedFloatAccess extends AbstractBufferedAccess
            implements FloatReadAccess, FloatWriteAccess {

            private float m_value;

            @Override
            public void setFloatValue(final float value) {
                m_value = value;
            }

            @Override
            public float getFloatValue() {
                return m_value;
            }

            @Override
            protected void setFromNonMissing(final ReadAccess access) {
                m_value = ((FloatReadAccess)access).getFloatValue();
            }

        }

        private static final class BufferedIntAccess extends AbstractBufferedAccess
            implements IntReadAccess, IntWriteAccess {

            private int m_value;

            @Override
            public int getIntValue() {
                return m_value;
            }

            @Override
            public void setIntValue(final int value) {
                m_value = value;
                m_isMissing = false;
            }

            @Override
            protected void setFromNonMissing(final ReadAccess access) {
                m_value = ((IntReadAccess)access).getIntValue();
            }

        }

        // TODO: this implementation looks pretty inefficient
        private static final class BufferedListAccess implements ListReadAccess, ListWriteAccess, BufferedAccess {

            private final ListDataSpec m_spec;

            private BufferedAccess[] m_inner;

            BufferedListAccess(final ListDataSpec spec) {
                m_spec = spec;
            }

            @Override
            public boolean isMissing() {
                return m_inner == null;
            }

            @Override
            public int size() {
                return m_inner.length;
            }

            @Override
            public <R extends ReadAccess> R getAccess(final int index) {
                @SuppressWarnings("unchecked")
                R access = (R)m_inner[index];
                return access;
            }

            @Override
            public void setMissing() {
                m_inner = null;
            }

            @Override
            public void create(final int size) {
                m_inner = new BufferedAccess[size];
                for (int i = 0; i < size; i++) {
                    m_inner[i] = createBufferedAccess(m_spec.getInner());
                }
            }

            @Override
            public <W extends WriteAccess> W getWriteAccess(final int index) {
                @SuppressWarnings("unchecked")
                final W access = (W)m_inner[index];
                return access;
            }

            @Override
            public void setFrom(final ReadAccess access) {
                if (access.isMissing()) {
                    setMissing();
                } else {
                    final ListReadAccess listAccess = (ListReadAccess)access;
                    final int listSize = listAccess.size();
                    create(listSize);
                    for (int i = 0; i < listSize; i++) {
                        m_inner[i].setFrom(listAccess.getAccess(i));
                    }
                }
            }

            @Override
            public boolean isMissing(final int index) {
                return m_inner[index].isMissing();
            }

            @Override
            public DataSpec getDataSpec() {
                return m_spec;
            }
        }

        private static final class BufferedLongAccess extends AbstractBufferedAccess
            implements LongReadAccess, LongWriteAccess {

            private long m_value;

            @Override
            public long getLongValue() {
                return m_value;
            }

            @Override
            public void setLongValue(final long value) {
                m_value = value;
                m_isMissing = false;
            }

            @Override
            protected void setFromNonMissing(final ReadAccess access) {
                m_value = ((LongReadAccess)access).getLongValue();
            }

        }

        private static final class BufferedStructAccess implements StructWriteAccess, StructReadAccess, BufferedAccess {

            private final BufferedAccess[] m_inner;

            BufferedStructAccess(final StructDataSpec spec) {
                m_inner = new BufferedAccess[spec.size()];
                Arrays.setAll(m_inner, i -> createBufferedAccess(spec.getDataSpec(i)));
            }

            @Override
            public boolean isMissing() {
                // if one value was set in any of the inner access, we consider the struct to be valid
                for (final BufferedAccess access : m_inner) {
                    if (!access.isMissing()) {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public int size() {
                return m_inner.length;
            }

            @Override
            public <R extends ReadAccess> R getAccess(final int index) {
                @SuppressWarnings("unchecked")
                final R cast = (R)m_inner[index];
                return cast;
            }

            @Override
            public void setMissing() {
                for (final BufferedAccess access : m_inner) {
                    access.setMissing();
                }
            }

            @Override
            public <W extends WriteAccess> W getWriteAccess(final int index) {
                @SuppressWarnings("unchecked")
                final W cast = (W)m_inner[index];
                return cast;
            }

            @Override
            public void setFrom(final ReadAccess access) {
                final StructReadAccess structAccess = (StructReadAccess)access;
                final int numInnerReadAccesses = structAccess.size();
                for (int i = 0; i < numInnerReadAccesses; i++) {
                    m_inner[i].setFrom(structAccess.getAccess(i));
                }
            }
        }

        private static final class BufferedLocalDateAccess extends AbstractBufferedObjectAccess<LocalDate>
            implements LocalDateReadAccess, LocalDateWriteAccess {

            @Override
            public void setLocalDateValue(final LocalDate value) {
                m_value = value;
            }

            @Override
            public LocalDate getLocalDateValue() {
                return m_value;
            }

            @Override
            protected void setFromNonMissing(final ReadAccess access) {
                m_value = ((LocalDateReadAccess)access).getLocalDateValue();
            }

        }

        private static final class BufferedLocalDateTimeAccess extends AbstractBufferedObjectAccess<LocalDateTime>
            implements LocalDateTimeReadAccess, LocalDateTimeWriteAccess {

            @Override
            public void setLocalDateTimeValue(final LocalDateTime value) {
                m_value = value;
            }

            @Override
            public LocalDateTime getLocalDateTimeValue() {
                return m_value;
            }

            @Override
            protected void setFromNonMissing(final ReadAccess access) {
                m_value = ((LocalDateTimeReadAccess)access).getLocalDateTimeValue();
            }

        }

        private static final class BufferedLocalTimeAccess extends AbstractBufferedObjectAccess<LocalTime>
            implements LocalTimeReadAccess, LocalTimeWriteAccess {

            @Override
            public void setLocalTimeValue(final LocalTime value) {
                m_value = value;
            }

            @Override
            public LocalTime getLocalTimeValue() {
                return m_value;
            }

            @Override
            protected void setFromNonMissing(final ReadAccess access) {
                m_value = ((LocalTimeReadAccess)access).getLocalTimeValue();
            }

        }

        private static final class BufferedPeriodAccess extends AbstractBufferedObjectAccess<Period>
            implements PeriodReadAccess, PeriodWriteAccess {

            @Override
            public void setPeriodValue(final Period value) {
                m_value = value;
            }

            @Override
            public Period getPeriodValue() {
                return m_value;
            }

            @Override
            protected void setFromNonMissing(final ReadAccess access) {
                m_value = ((PeriodReadAccess)access).getPeriodValue();
            }

        }

        private static final class BufferedStringAccess extends AbstractBufferedObjectAccess<String>
            implements StringReadAccess, StringWriteAccess {

            @Override
            public void setStringValue(final String value) {
                m_value = value;
            }

            @Override
            public String getStringValue() {
                return m_value;
            }

            @Override
            protected void setFromNonMissing(final ReadAccess access) {
                m_value = ((StringReadAccess)access).getStringValue();
            }

        }

        private static final class BufferedVarBinaryAccess extends AbstractBufferedAccess
            implements VarBinaryReadAccess, VarBinaryWriteAccess {

            private Object m_value;

            @Override
            public void setByteArray(final byte[] value) {
                m_value = value;
                m_isMissing = false;
            }

            @Override
            public void setByteArray(final byte[] array, final int index, final int length) {
                m_value = new byte[length];
                System.arraycopy(array, index, m_value, 0, length);
                m_isMissing = false;
            }

            @Override
            public byte[] getByteArray() {
                return (byte[])m_value;
            }

            @Override
            public <T> void setObject(final T value, final ObjectSerializer<T> serializer) {
                // TODO: should we write a DataOutput that writes into a byte array instead?
                m_value = value;
                m_isMissing = false;
            }

            @Override
            public <T> T getObject(final ObjectDeserializer<T> deserializer) {
                @SuppressWarnings("unchecked")
                T value = (T)m_value;
                return value;
            }

            @Override
            protected void setFromNonMissing(final ReadAccess access) {
                m_value = ((VarBinaryReadAccess)access).getByteArray();
            }

        }

        private static final class BufferedVoidAccess implements BufferedAccess {

            private static final BufferedVoidAccess VOID_ACCESS_INSTANCE = new BufferedVoidAccess();

            private BufferedVoidAccess() {
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
            public DataSpec getDataSpec() {
                return DataSpec.voidSpec();
            }
        }

        private static final class BufferedZonedDateTimeAccess extends AbstractBufferedObjectAccess<ZonedDateTime>
            implements ZonedDateTimeReadAccess, ZonedDateTimeWriteAccess {

            @Override
            public void setZonedDateTimeValue(final ZonedDateTime value) {
                m_value = value;
            }

            @Override
            public ZonedDateTime getZonedDateTimeValue() {
                return m_value;
            }

            @Override
            protected void setFromNonMissing(final ReadAccess access) {
                m_value = ((ZonedDateTimeReadAccess)access).getZonedDateTimeValue();
            }

        }

        private abstract static class AbstractBufferedAccess implements BufferedAccess {

            protected boolean m_isMissing = true;

            @Override
            public boolean isMissing() {
                return m_isMissing;
            }

            @Override
            public void setMissing() {
                m_isMissing = true;
            }

            @Override
            public void setFrom(final ReadAccess access) {
                if (access.isMissing()) {
                    setMissing(); // Overridden by BufferedVarBinaryAccess.
                } else {
                    m_isMissing = false;
                    setFromNonMissing(access);
                }
            }

            protected abstract void setFromNonMissing(ReadAccess access);

        }

        private abstract static class AbstractBufferedObjectAccess<T> implements BufferedAccess {

            protected T m_value;

            @Override
            public final boolean isMissing() {
                return m_value == null;
            }

            @Override
            public final void setMissing() {
                m_value = null;
            }

            @Override
            public void setFrom(final ReadAccess access) {
                if (access.isMissing()) {
                    m_value = null;
                } else {
                    setFromNonMissing(access);
                }
            }

            protected abstract void setFromNonMissing(ReadAccess access);

        }
    }
}