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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.knime.core.table.access.BooleanAccess.BooleanReadAccess;
import org.knime.core.table.access.BooleanAccess.BooleanWriteAccess;
import org.knime.core.table.access.ByteAccess.ByteReadAccess;
import org.knime.core.table.access.ByteAccess.ByteWriteAccess;
import org.knime.core.table.access.DelegatingReadAccesses.DelegatingReadAccess;
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
import org.knime.core.table.io.ReadableDataInputStream;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.Selection.ColumnSelection;
import org.knime.core.table.row.WriteAccessRow;
import org.knime.core.table.schema.BooleanDataSpec;
import org.knime.core.table.schema.ByteDataSpec;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.DoubleDataSpec;
import org.knime.core.table.schema.FloatDataSpec;
import org.knime.core.table.schema.IntDataSpec;
import org.knime.core.table.schema.ListDataSpec;
import org.knime.core.table.schema.LongDataSpec;
import org.knime.core.table.schema.StringDataSpec;
import org.knime.core.table.schema.StructDataSpec;
import org.knime.core.table.schema.VarBinaryDataSpec;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectDeserializer;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectSerializer;
import org.knime.core.table.schema.VoidDataSpec;

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
     * Creates a {@link BufferedAccessRow} with the provided {@link ColumnarSchema}. {@code BufferedAccess}es of
     * non-selected columns will be {@code null} in the returned {@code BufferedAccessRow}.
     *
     * @param schema defining the number of columns and their types
     * @param columnSelection selected columns
     * @return a {@link BufferedAccessRow} with the provided {@link ColumnarSchema}
     */
    public static BufferedAccessRow createBufferedAccessRow(final ColumnarSchema schema,
        final ColumnSelection columnSelection) {
        return new DefaultBufferedAccessRow(schema, columnSelection);
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
            m_accesses = new BufferedAccess[schema.numColumns()];
            Arrays.setAll(m_accesses, i -> createBufferedAccess(schema.getSpec(i)));
        }

        private DefaultBufferedAccessRow(final ColumnarSchema schema, final ColumnSelection columnSelection) {
            m_accesses = new BufferedAccess[schema.numColumns()];
            Arrays.setAll(m_accesses, i -> columnSelection.isSelected(i) //
                ? createBufferedAccess(schema.getSpec(i)) //
                : null);
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
                final BufferedAccess access = m_accesses[i];
                if (access != null) {
                    access.setFrom(readAccessRow.getAccess(i));
                }
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

        @Override
        public String toString() {
            return Arrays.toString(m_accesses);
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

            @Override
            protected String valueToString() {
                return Boolean.toString(m_value);
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

            @Override
            protected String valueToString() {
                return Byte.toString(m_value);
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

            @Override
            protected String valueToString() {
                return Double.toString(m_value);
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

            @Override
            protected String valueToString() {
                return Float.toString(m_value);
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

            @Override
            protected String valueToString() {
                return Integer.toString(m_value);
            }

        }

        private static final class BufferedListAccess extends AbstractBufferedAccess
            implements ListReadAccess, ListWriteAccess {

            private final ListDataSpec m_spec;

            private BufferedAccess[] m_inner = new BufferedAccess[0];

            private int m_size;

            private final DelegatingReadAccess m_readAccess;

            private final DelegatingWriteAccess m_writeAccess;

            BufferedListAccess(final ListDataSpec spec) {
                m_spec = spec;
                m_readAccess = DelegatingReadAccesses.createDelegatingAccess(spec.getInner());
                m_writeAccess = DelegatingWriteAccesses.createDelegatingWriteAccess(spec.getInner());
            }

            @Override
            public int size() {
                return m_size;
            }

            @SuppressWarnings("unchecked")
            @Override
            public <R extends ReadAccess> R getAccess() {
                return (R)m_readAccess;
            }

            @Override
            public void setIndex(final int index) {
                checkIndex(index);
                m_readAccess.setDelegateAccess(m_inner[index]);
            }

            private void checkIndex(final int index) {
                if (index < 0 || index >= size()) {
                    throw new IndexOutOfBoundsException();
                }
            }

            @Override
            public void setWriteIndex(final int index) {
                checkIndex(index);
                m_writeAccess.setDelegateAccess(m_inner[index]);
            }

            @Override
            public void create(final int size) {
                m_isMissing = false;
                m_size = size;
                for (var buffer : m_inner) {
                    buffer.setMissing();
                }
                if (m_inner.length < size) {
                    var newInner = Arrays.copyOf(m_inner, size);
                    for (int i = 0; i < m_inner.length; i++) {//NOSONAR
                        newInner[i].setMissing();
                    }
                    for (int i = m_inner.length; i < size; i++) {//NOSONAR
                        newInner[i] = createInnerBuffer();
                    }
                    m_inner = newInner;
                } else {
                    // reuse existing buffers
                }
            }

            private BufferedAccess createInnerBuffer() {
                return createBufferedAccess(m_spec.getInner());
            }

            @Override
            public <W extends WriteAccess> W getWriteAccess() {
                @SuppressWarnings("unchecked")
                final W access = (W)m_writeAccess;
                return access;
            }

            @Override
            public boolean isMissing(final int index) {
                checkIndex(index);
                return m_inner[index].isMissing();
            }

            @Override
            public DataSpec getDataSpec() {
                return m_spec;
            }

            @Override
            protected void setFromNonMissing(final ReadAccess access) {
                final ListReadAccess listAccess = (ListReadAccess)access;
                final int listSize = listAccess.size();
                create(listSize);
                final var elementAccess = listAccess.getAccess();
                for (int i = 0; i < listSize; i++) {//NOSONAR
                    listAccess.setIndex(i);
                    m_inner[i].setFrom(elementAccess);
                }
            }

            @Override
            protected String valueToString() {
                return Stream.of(m_inner)//
                    .limit(m_size)//
                    .map(Object::toString)//
                    .collect(Collectors.joining(",", "[", "]"));
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

            @Override
            protected String valueToString() {
                return Long.toString(m_value);
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
                for (int i = 0; i < numInnerReadAccesses; i++) {//NOSONAR
                    m_inner[i].setFrom(structAccess.getAccess(i));
                }
            }

            @Override
            public String toString() {
                var sb = new StringBuilder("{");
                for (int i = 0; i < m_inner.length; i++) {//NOSONAR
                    sb.append(i).append(": ");
                    sb.append(m_inner[i]);
                }
                sb.append("}");
                return sb.toString();
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

            // The value is only a cache in case setObject or getObject were called
            private Object m_value;

            // The object serializer, cached for on-demand conversion to bytes.
            @SuppressWarnings("rawtypes")
            private ObjectSerializer m_serializer;

            // The storage bytes. get/setByteArray will operate on those bytes.
            private byte[] m_storage;

            /**
             * @return Retrieves the bytes either from the storage bytes, or by serializing the cached object. This
             *         allows us to call serialize() only in case the bytes are needed.
             */
            @SuppressWarnings("unchecked")
            private byte[] getStorage() {
                if (m_storage == null) {
                    if (m_serializer == null) { // the object could really have the value null, so we can only check for the serializer
                        throw new IllegalStateException(
                            "Cannot retrieve storage bytes from an object without a serializer set");
                    }

                    try {
                        var outStream = new ByteArrayOutputStream();
                        m_serializer.serialize(new DataOutputStream(outStream), m_value);
                        m_storage = outStream.toByteArray();
                    } catch (IOException e) {
                        throw new IllegalArgumentException("Serializer failed to serialize the given object.", e);
                    }
                }

                return m_storage;
            }

            @Override
            public void setByteArray(final byte[] value) {
                m_storage = value;
                m_value = null;
                m_serializer = null;
                m_isMissing = false;
            }

            @Override
            public void setByteArray(final byte[] array, final int index, final int length) {
                m_storage = new byte[length];
                System.arraycopy(array, index, m_storage, 0, length);
                m_isMissing = false;
                m_value = null;
                m_serializer = null;
            }

            @Override
            public byte[] getByteArray() {
                return getStorage();
            }

            @Override
            public <T> void setObject(final T value, final ObjectSerializer<T> serializer) {
                // we only populate m_storage if getByteArray() is accessed after an object was set.
                m_serializer = serializer;
                m_storage = null;
                m_value = value;
                m_isMissing = false;
            }

            @Override
            public <T> T getObject(final ObjectDeserializer<T> deserializer) {
                if (m_value != null) {
                    @SuppressWarnings("unchecked")
                    var casted = (T)m_value;
                    return casted;
                } else if (m_storage != null) {
                    try {
                        T object = //NOSONAR
                            deserializer.deserialize(new ReadableDataInputStream(new ByteArrayInputStream(m_storage)));
                        m_value = object;
                        return object;
                    } catch (IOException e) {
                        throw new IllegalArgumentException("Deserializer failed to deserialize the stored object.", e);
                    }
                } else {
                    throw new IllegalStateException("Neither a value nor a byte array has been set.");
                }

            }

            @Override
            protected void setFromNonMissing(final ReadAccess access) {
                m_storage = ((VarBinaryReadAccess)access).getByteArray();
                m_value = null;
                m_isMissing = false;
            }

            @Override
            protected String valueToString() {
                return m_value != null ? m_value.toString() : (m_storage != null ? m_storage.toString() : "null");
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

            @Override
            public final String toString() {
                return m_isMissing ? "?" : valueToString();
            }

            protected abstract String valueToString();

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

            @Override
            public String toString() {
                return isMissing() ? "?" : m_value.toString();
            }

        }
    }
}