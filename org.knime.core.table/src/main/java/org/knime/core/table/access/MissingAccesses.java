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

import java.util.Arrays;

import org.knime.core.table.access.BooleanAccess.BooleanReadAccess;
import org.knime.core.table.access.ByteAccess.ByteReadAccess;
import org.knime.core.table.access.DoubleAccess.DoubleReadAccess;
import org.knime.core.table.access.FloatAccess.FloatReadAccess;
import org.knime.core.table.access.IntAccess.IntReadAccess;
import org.knime.core.table.access.ListAccess.ListReadAccess;
import org.knime.core.table.access.LongAccess.LongReadAccess;
import org.knime.core.table.access.StringAccess.StringReadAccess;
import org.knime.core.table.access.StructAccess.StructReadAccess;
import org.knime.core.table.access.VarBinaryAccess.VarBinaryReadAccess;
import org.knime.core.table.row.ReadAccessRow;
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
import org.knime.core.table.schema.VoidDataSpec;
import org.knime.core.table.row.DefaultReadAccessRow;

/**
 * A collection of access implementations that are all {@code isMissing} and can be
 * retrieved by mapping from a given {@link DataSpec}.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Tobias Pietzsch
 */
public final class MissingAccesses {

    private MissingAccesses() {
    }

    /**
     * Get a {@link MissingAccess} for the provided {@link DataSpec}.
     *
     * @param spec for which a {@link MissingAccess} is required
     * @return a {@link MissingAccess} for the provided {@link DataSpec}
     */
    public static MissingAccess getMissingAccess(final DataSpec spec) {
        return spec.accept(DataSpecToMissingAccessMapper.INSTANCE);
    }

    /**
     * Get {@link ReadAccessRow} of all {@link MissingAccess MissingAccesses} with the provided {@link ColumnarSchema}.
     *
     * @param schema of the table
     * @return the missing ReadAccessRow
     */
    public static ReadAccessRow getMissingReadAccessRow(final ColumnarSchema schema) {
        return new DefaultReadAccessRow(schema.numColumns(), i -> getMissingAccess(schema.getSpec(i)));
    }


    /**
     * A {@link ReadAccess} which {@code isMissing()} by default.
     */
    public interface MissingAccess extends ReadAccess {
        @Override
        default boolean isMissing() {
            return true;
        }
    }


    private static final class DataSpecToMissingAccessMapper implements DataSpec.Mapper<MissingAccess> {

        private static final DataSpecToMissingAccessMapper INSTANCE = new DataSpecToMissingAccessMapper();

        @Override
        public MissingAccess visit(final ListDataSpec spec) {
            return new MissingListAccess(spec);
        }

        @Override
        public MissingAccess visit(final StructDataSpec spec) {
            return new MissingStructAccess(spec);
        }

        @Override
        public MissingAccess visit(final DoubleDataSpec spec) {
            return MissingDoubleAccess.INSTANCE;
        }

        @Override
        public MissingAccess visit(final BooleanDataSpec spec) {
            return MissingBooleanAccess.INSTANCE;
        }

        @Override
        public MissingAccess visit(final IntDataSpec spec) {
            return MissingIntAccess.INSTANCE;
        }

        @Override
        public MissingAccess visit(final LongDataSpec spec) {
            return MissingLongAccess.INSTANCE;
        }

        @Override
        public MissingAccess visit(final VoidDataSpec spec) {
            return MissingVoidAccess.INSTANCE;
        }

        @Override
        public MissingAccess visit(final VarBinaryDataSpec spec) {
            return MissingVarBinaryAccess.INSTANCE;
        }

        @Override
        public MissingAccess visit(final StringDataSpec spec) {
            return MissingStringAccess.INSTANCE;
        }

        @Override
        public MissingAccess visit(final ByteDataSpec spec) {
            return MissingByteAccess.INSTANCE;
        }

        @Override
        public MissingAccess visit(final FloatDataSpec spec) {
            return MissingFloatAccess.INSTANCE;
        }

        private static final class MissingBooleanAccess implements MissingAccess, BooleanReadAccess {

            static final MissingBooleanAccess INSTANCE = new MissingBooleanAccess();

            @Override
            public boolean getBooleanValue() {
                return false;
            }
        }

        private static final class MissingByteAccess implements MissingAccess, ByteReadAccess {

            static final MissingByteAccess INSTANCE = new MissingByteAccess();

            @Override
            public byte getByteValue() {
                return 0;
            }
        }

        private static final class MissingDoubleAccess implements MissingAccess, DoubleReadAccess {

            static final MissingDoubleAccess INSTANCE = new MissingDoubleAccess();

            @Override
            public double getDoubleValue() {
                return 0;
            }
        }

        private static final class MissingFloatAccess implements MissingAccess, FloatReadAccess {

            static final MissingFloatAccess INSTANCE = new MissingFloatAccess();

            @Override
            public float getFloatValue() {
                return 0;
            }
        }

        private static final class MissingIntAccess implements MissingAccess, IntReadAccess {

            static final MissingIntAccess INSTANCE = new MissingIntAccess();

            @Override
            public int getIntValue() {
                return 0;
            }
        }

        private static final class MissingLongAccess implements MissingAccess, LongReadAccess {

            static final MissingLongAccess INSTANCE = new MissingLongAccess();

            @Override
            public long getLongValue() {
                return 0;
            }
        }

        private static final class MissingStringAccess implements MissingAccess, StringReadAccess {

            static final MissingStringAccess INSTANCE = new MissingStringAccess();

            @Override
            public String getStringValue() {
                return null;
            }
        }

        private static final class MissingVarBinaryAccess implements MissingAccess, VarBinaryReadAccess {

            static final MissingVarBinaryAccess INSTANCE = new MissingVarBinaryAccess();

            @Override
            public byte[] getByteArray() {
                return null;
            }

            @Override
            public <T> T getObject(final ObjectDeserializer<T> deserializer) {
                return null;
            }
        }

        private static final class MissingVoidAccess implements MissingAccess {

            static final MissingVoidAccess INSTANCE = new MissingVoidAccess();

            @Override
            public DataSpec getDataSpec() {
                return DataSpec.voidSpec();
            }
        }

        private static final class MissingListAccess implements ListReadAccess, MissingAccess {

            private final ListDataSpec m_spec;

            private final MissingAccess m_elementAccess;

            MissingListAccess(final ListDataSpec spec) {
                m_spec = spec;
                m_elementAccess = getMissingAccess(spec.getInner());
            }

            @Override
            public int size() {
                return 0;
            }

            @SuppressWarnings("unchecked")
            @Override
            public <R extends ReadAccess> R getAccess() {
                return (R)m_elementAccess;
            }

            @Override
            public void setIndex(final int index) {
                // nothing to do
            }

            @Override
            public boolean isMissing(final int index) {
                return true;
            }

            @Override
            public DataSpec getDataSpec() {
                return m_spec;
            }
        }

        private static final class MissingStructAccess implements StructReadAccess, MissingAccess {

            private final StructDataSpec m_spec;

            private final MissingAccess[] m_inner;

            MissingStructAccess(final StructDataSpec spec) {
                m_spec = spec;
                m_inner = new MissingAccess[spec.size()];
                Arrays.setAll(m_inner, i -> getMissingAccess(spec.getDataSpec(i)));
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
            public DataSpec getDataSpec() {
                return m_spec;
            }
        }
    }
}
