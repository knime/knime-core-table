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
 *   Created on 29 Oct 2021 by Steffen Fissler, KNIME GmbH, Konstanz, Germany
 */
package org.knime.core.table.access;

import java.util.Arrays;

import org.knime.core.table.access.BooleanAccess.BooleanWriteAccess;
import org.knime.core.table.access.ByteAccess.ByteWriteAccess;
import org.knime.core.table.access.DoubleAccess.DoubleWriteAccess;
import org.knime.core.table.access.FloatAccess.FloatWriteAccess;
import org.knime.core.table.access.IntAccess.IntWriteAccess;
import org.knime.core.table.access.ListAccess.ListWriteAccess;
import org.knime.core.table.access.LongAccess.LongWriteAccess;
import org.knime.core.table.access.StringAccess.StringWriteAccess;
import org.knime.core.table.access.StructAccess.StructWriteAccess;
import org.knime.core.table.access.VarBinaryAccess.VarBinaryWriteAccess;
import org.knime.core.table.schema.BooleanDataSpec;
import org.knime.core.table.schema.ByteDataSpec;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.DoubleDataSpec;
import org.knime.core.table.schema.FloatDataSpec;
import org.knime.core.table.schema.IntDataSpec;
import org.knime.core.table.schema.ListDataSpec;
import org.knime.core.table.schema.LongDataSpec;
import org.knime.core.table.schema.StringDataSpec;
import org.knime.core.table.schema.StructDataSpec;
import org.knime.core.table.schema.VarBinaryDataSpec;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectSerializer;
import org.knime.core.table.schema.VoidDataSpec;

/**
 * Provides implementations of {@link WriteAccess WriteAccesses} that simply delegate to another {@link WriteAccess}.
 *
 * @author Steffen Fissler, KNIME GmbH, Konstanz, Germany
 */
public final class DelegatingWriteAccesses {

    private DelegatingWriteAccesses() {
    }

    /**
     * @param spec the type of WriteAccess
     * @return a {@link DelegatingWriteAccess} with the provided {@link DataSpec}
     */
    public static DelegatingWriteAccess createDelegatingWriteAccess(final DataSpec spec) {
        return spec.accept(DataSpecToDelegatingWriteAccessMapper.INSTANCE);
    }

    /**
     * A {@link WriteAccess} that delegates to another {@link WriteAccess} of the same type.
     *
     * @author Steffen Fissler, KNIME GmbH, Konstanz, Germany
     */
    public static interface DelegatingWriteAccess extends WriteAccess {
        /**
         * Sets the access this write access delegates to.
         *
         * @param access to delegate to
         */
        void setDelegateAccess(WriteAccess access);
    }

    private static final class DataSpecToDelegatingWriteAccessMapper implements DataSpec.Mapper<DelegatingWriteAccess> {

        private static final DataSpecToDelegatingWriteAccessMapper INSTANCE =
            new DataSpecToDelegatingWriteAccessMapper();

        @Override
        public DelegatingWriteAccess visit(final BooleanDataSpec spec) {
            return new DelegatingBooleanWriteAccess(spec);
        }

        @Override
        public DelegatingWriteAccess visit(final ByteDataSpec spec) {
            return new DelegatingByteWriteAccess(spec);
        }

        @Override
        public DelegatingWriteAccess visit(final DoubleDataSpec spec) {
            return new DelegatingDoubleWriteAccess(spec);
        }

        @Override
        public DelegatingWriteAccess visit(final FloatDataSpec spec) {
            return new DelegatingFloatWriteAccess(spec);
        }

        @Override
        public DelegatingWriteAccess visit(final IntDataSpec spec) {
            return new DelegatingIntWriteAccess(spec);
        }

        @Override
        public DelegatingWriteAccess visit(final LongDataSpec spec) {
            return new DelegatingLongWriteAccess(spec);
        }

        @Override
        public DelegatingWriteAccess visit(final VarBinaryDataSpec spec) {
            return new DelegatingVarBinaryWriteAccess(spec);
        }

        @Override
        public DelegatingWriteAccess visit(final StructDataSpec spec) {
            return new DelegatingStructWriteAccess(spec);
        }

        @Override
        public DelegatingWriteAccess visit(final ListDataSpec spec) {
            return new DelegatingListWriteAccess(spec);
        }

        @Override
        public DelegatingWriteAccess visit(final StringDataSpec spec) {
            return new DelegatingStringWriteAccess(spec);
        }

        @Override
        public DelegatingWriteAccess visit(final VoidDataSpec spec) {
            return new AbstractDelegatingWriteAccess<WriteAccess>(spec) {
            };
        }
    }

    private static final class DelegatingBooleanWriteAccess extends AbstractDelegatingWriteAccess<BooleanWriteAccess>
        implements BooleanWriteAccess {

        private DelegatingBooleanWriteAccess(final DataSpec spec) {
            super(spec);
        }

        @Override
        public void setBooleanValue(final boolean value) {
            m_delegateAccess.setBooleanValue(value);
        }
    }

    private static final class DelegatingByteWriteAccess extends AbstractDelegatingWriteAccess<ByteWriteAccess>
        implements ByteWriteAccess {

        private DelegatingByteWriteAccess(final DataSpec spec) {
            super(spec);
        }

        @Override
        public void setByteValue(final byte value) {
            m_delegateAccess.setByteValue(value);
        }
    }

    private static final class DelegatingDoubleWriteAccess extends AbstractDelegatingWriteAccess<DoubleWriteAccess>
        implements DoubleWriteAccess {

        private DelegatingDoubleWriteAccess(final DataSpec spec) {
            super(spec);
        }

        @Override
        public void setDoubleValue(final double value) {
            m_delegateAccess.setDoubleValue(value);
        }
    }

    private static final class DelegatingFloatWriteAccess extends AbstractDelegatingWriteAccess<FloatWriteAccess>
        implements FloatWriteAccess {

        private DelegatingFloatWriteAccess(final DataSpec spec) {
            super(spec);
        }

        @Override
        public void setFloatValue(final float value) {
            m_delegateAccess.setFloatValue(value);
        }
    }

    private static final class DelegatingIntWriteAccess extends AbstractDelegatingWriteAccess<IntWriteAccess>
        implements IntWriteAccess {

        private DelegatingIntWriteAccess(final DataSpec spec) {
            super(spec);
        }

        @Override
        public void setIntValue(final int value) {
            m_delegateAccess.setIntValue(value);
        }
    }

    private static final class DelegatingListWriteAccess extends AbstractDelegatingWriteAccess<ListWriteAccess>
        implements ListWriteAccess {

        private final DelegatingWriteAccess m_innerAccess;

        public DelegatingListWriteAccess(final ListDataSpec spec) {
            super(spec);
            m_innerAccess = createDelegatingWriteAccess(spec.getInner());
        }

        @Override
        public void setDelegateAccess(final WriteAccess access) {
            super.setDelegateAccess(access);
            m_innerAccess.setDelegateAccess(m_delegateAccess.getWriteAccess());
        }

        @SuppressWarnings("unchecked")
        @Override
        public <A extends WriteAccess> A getWriteAccess() {
            return (A)m_innerAccess;
        }

        @Override
        public void setWriteIndex(final int index) {
            m_delegateAccess.setWriteIndex(index);
        }

        @Override
        public void create(final int size) {
            m_delegateAccess.create(size);
        }

    }

    private static final class DelegatingLongWriteAccess extends AbstractDelegatingWriteAccess<LongWriteAccess>
        implements LongWriteAccess {

        private DelegatingLongWriteAccess(final DataSpec spec) {
            super(spec);
        }

        @Override
        public void setLongValue(final long value) {
            m_delegateAccess.setLongValue(value);
        }
    }

    private static final class DelegatingStringWriteAccess extends AbstractDelegatingWriteAccess<StringWriteAccess>
        implements StringWriteAccess {

        private DelegatingStringWriteAccess(final DataSpec spec) {
            super(spec);
        }

        @Override
        public void setStringValue(final String value) {
            m_delegateAccess.setStringValue(value);
        }
    }

    private static final class DelegatingStructWriteAccess extends AbstractDelegatingWriteAccess<StructWriteAccess>
        implements StructWriteAccess {

        private final DelegatingWriteAccess[] m_accesses;

        private DelegatingStructWriteAccess(final StructDataSpec spec) {
            super(spec);
            m_accesses = new DelegatingWriteAccess[spec.size()];
            Arrays.setAll(m_accesses, i -> createDelegatingWriteAccess(spec.getDataSpec(i)));
        }

        @Override
        public void setDelegateAccess(final WriteAccess access) {
            super.setDelegateAccess(access);
            for (int i = 0; i < m_accesses.length; i++) { // NOSONAR
                m_accesses[i].setDelegateAccess(m_delegateAccess.getWriteAccess(i));
            }
        }

        @Override
        public int size() {
            return m_accesses.length;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <W extends WriteAccess> W getWriteAccess(final int index) {
            return (W)m_accesses[index];
        }
    }

    private static final class DelegatingVarBinaryWriteAccess
        extends AbstractDelegatingWriteAccess<VarBinaryWriteAccess> implements VarBinaryWriteAccess {

        public DelegatingVarBinaryWriteAccess(final DataSpec spec) {
            super(spec);
        }

        @Override
        public void setByteArray(final byte[] value) {
            m_delegateAccess.setByteArray(value);
        }

        @Override
        public void setByteArray(final byte[] array, final int index, final int length) {
            m_delegateAccess.setByteArray(array, index, length);
        }

        @Override
        public <T> void setObject(final T value, final ObjectSerializer<T> serializer) {
            m_delegateAccess.setObject(value, serializer);
        }
    }

    @SuppressWarnings("unused")
    private abstract static class AbstractDelegatingWriteAccess<A extends WriteAccess>
        implements DelegatingWriteAccess {
        private final DataSpec m_spec;

        protected A m_delegateAccess;

        public AbstractDelegatingWriteAccess(final DataSpec spec) {
            m_spec = spec;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void setDelegateAccess(final WriteAccess access) {
            m_delegateAccess = (A)access;
        }

        @Override
        public String toString() {
            return m_delegateAccess.toString();
        }

        @Override
        public void setFrom(final ReadAccess access) {
            m_delegateAccess.setFrom(access);
        }

        @Override
        public void setMissing() {
            if (m_delegateAccess != null) {
                m_delegateAccess.setMissing();
            }
        }

    }

}
