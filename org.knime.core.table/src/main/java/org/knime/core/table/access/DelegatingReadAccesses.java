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

/**
 * Provides implementations of {@link ReadAccess ReadAccesses} that simply delegate to another {@link ReadAccess}.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 * @author Steffen Fissler, KNIME GmbH, Konstanz, Germany
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
    public static DelegatingReadAccess createDelegatingAccess(final DataSpec spec) {
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

        private final DelegatingReadAccess[] m_accesses;

        private final ColumnarSchema m_schema;

        DefaultDelegatingReadAccessRow(final ColumnarSchema schema) {
            m_schema = schema;
            m_accesses = new DelegatingReadAccess[m_schema.numColumns()];
            for (int i = 0; i < m_accesses.length; i++) {//NOSONAR
                m_accesses[i] = DelegatingReadAccesses.createDelegatingAccess(m_schema.getSpec(i));
            }
        }

        @Override
        public void setDelegateAccess(final ReadAccessRow delegateAccess) {
            for (int i = 0; i < m_schema.numColumns(); i++) {//NOSONAR
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
            final A casted = (A)m_accesses[index];//NOSONAR
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
     */
    public static interface DelegatingReadAccess extends ReadAccess {

        /**
         * Sets the access this access delegates to.
         *
         * @param access to delegate to
         */
        void setDelegateAccess(ReadAccess access);
    }

    private static final class DataSpecToDelegatingReadAccessMapper implements DataSpec.Mapper<DelegatingReadAccess> {

        private static final DataSpecToDelegatingReadAccessMapper INSTANCE = new DataSpecToDelegatingReadAccessMapper();

        @Override
        public DelegatingReadAccess visit(final BooleanDataSpec spec) {
            return new DelegatingBooleanReadAccess(spec);
        }

        @Override
        public DelegatingReadAccess visit(final ByteDataSpec spec) {
            return new DelegatingByteReadAccess(spec);
        }

        @Override
        public DelegatingReadAccess visit(final DoubleDataSpec spec) {
            return new DelegatingDoubleReadAccess(spec);
        }

        @Override
        public DelegatingReadAccess visit(final FloatDataSpec spec) {
            return new DelegatingFloatReadAccess(spec);
        }

        @Override
        public DelegatingReadAccess visit(final IntDataSpec spec) {
            return new DelegatingIntReadAccess(spec);
        }

        @Override
        public DelegatingReadAccess visit(final LongDataSpec spec) {
            return new DelegatingLongReadAccess(spec);
        }

        @Override
        public DelegatingReadAccess visit(final VarBinaryDataSpec spec) {
            return new DelegatingVarBinaryReadAccess(spec);
        }

        @Override
        public DelegatingReadAccess visit(final VoidDataSpec spec) {
            return new AbstractDelegatingReadAccess<ReadAccess>(spec) {
            };
        }

        @Override
        public DelegatingReadAccess visit(final StructDataSpec spec) {
            return new DelegatingStructReadAccess(spec);
        }

        @Override
        public DelegatingReadAccess visit(final ListDataSpec spec) {
            return new DelegatingListReadAccess(spec);
        }

        @Override
        public DelegatingReadAccess visit(final StringDataSpec spec) {
            return new DelegatingStringReadAccess(spec);
        }
    }

    private static final class DelegatingBooleanReadAccess extends AbstractDelegatingReadAccess<BooleanReadAccess>
        implements BooleanReadAccess {

        private DelegatingBooleanReadAccess(final DataSpec spec) {
            super(spec);
        }

        @Override
        public boolean getBooleanValue() {
            return m_delegateAccess.getBooleanValue();
        }
    }

    private static final class DelegatingByteReadAccess extends AbstractDelegatingReadAccess<ByteReadAccess>
        implements ByteReadAccess {

        private DelegatingByteReadAccess(final DataSpec spec) {
            super(spec);
        }

        @Override
        public byte getByteValue() {
            return m_delegateAccess.getByteValue();
        }
    }

    private static final class DelegatingDoubleReadAccess extends AbstractDelegatingReadAccess<DoubleReadAccess>
        implements DoubleReadAccess {

        private DelegatingDoubleReadAccess(final DataSpec spec) {
            super(spec);
        }

        @Override
        public double getDoubleValue() {
            return m_delegateAccess.getDoubleValue();
        }
    }

    private static final class DelegatingFloatReadAccess extends AbstractDelegatingReadAccess<FloatReadAccess>
        implements FloatReadAccess {

        private DelegatingFloatReadAccess(final DataSpec spec) {
            super(spec);
        }

        @Override
        public float getFloatValue() {
            return m_delegateAccess.getFloatValue();
        }
    }

    private static final class DelegatingIntReadAccess extends AbstractDelegatingReadAccess<IntReadAccess>
        implements IntReadAccess {

        private DelegatingIntReadAccess(final DataSpec spec) {
            super(spec);
        }

        @Override
        public int getIntValue() {
            return m_delegateAccess.getIntValue();
        }
    }

    private static final class DelegatingListReadAccess extends AbstractDelegatingReadAccess<ListReadAccess>
        implements ListReadAccess {

        private final DelegatingReadAccess m_innerAccess;

        private DelegatingListReadAccess(final ListDataSpec spec) {
            super(spec);
            m_innerAccess = createDelegatingAccess(spec.getInner());
        }

        @Override
        public int size() {
            if (m_delegateAccess == null) {
                return 0;
            }
            return m_delegateAccess.size();
        }

        @SuppressWarnings("unchecked")
        @Override
        public <R extends ReadAccess> R getAccess() {
            return (R)m_innerAccess;
        }

        @Override
        public void setIndex(final int index) {
            m_delegateAccess.setIndex(index);
        }

        @Override
        public boolean isMissing(final int index) {
            return m_delegateAccess.isMissing(index);
        }

        @Override
        public void setDelegateAccess(final ReadAccess access) {
            super.setDelegateAccess(access);
            m_innerAccess.setDelegateAccess(m_delegateAccess.getAccess());
        }
    }

    private static final class DelegatingLongReadAccess extends AbstractDelegatingReadAccess<LongReadAccess>
        implements LongReadAccess {

        private DelegatingLongReadAccess(final DataSpec spec) {
            super(spec);
        }

        @Override
        public long getLongValue() {
            return m_delegateAccess.getLongValue();
        }
    }

    private static final class DelegatingStringReadAccess extends AbstractDelegatingReadAccess<StringReadAccess>
        implements StringReadAccess {

        private DelegatingStringReadAccess(final DataSpec spec) {
            super(spec);
        }

        @Override
        public String getStringValue() {
            return m_delegateAccess.getStringValue();
        }
    }

    private static final class DelegatingStructReadAccess extends AbstractDelegatingReadAccess<StructReadAccess>
        implements StructReadAccess {

        private final DelegatingReadAccess[] m_accesses;

        DelegatingStructReadAccess(final StructDataSpec spec) {
            super(spec);
            m_accesses = new DelegatingReadAccess[spec.size()];
            Arrays.setAll(m_accesses, i -> createDelegatingAccess(spec.getDataSpec(i)));
        }

        @Override
        public int size() {
            return m_accesses.length;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <R extends ReadAccess> R getAccess(final int index) {
            return (R)m_accesses[index];
        }

        @Override
        public void setDelegateAccess(final ReadAccess access) {
            super.setDelegateAccess(access);
            for (int i = 0; i < m_accesses.length; i++) { //NOSONAR
                m_accesses[i].setDelegateAccess(m_delegateAccess.getAccess(i));
            }
        }
    }

    private static final class DelegatingVarBinaryReadAccess extends AbstractDelegatingReadAccess<VarBinaryReadAccess>
        implements VarBinaryReadAccess {

        private DelegatingVarBinaryReadAccess(final DataSpec spec) {
            super(spec);
        }

        @Override
        public byte[] getByteArray() {
            return m_delegateAccess.getByteArray();
        }

        @Override
        public <T> T getObject(final ObjectDeserializer<T> deserializer) {
            return m_delegateAccess.getObject(deserializer);
        }
    }

    private abstract static class AbstractDelegatingReadAccess<A extends ReadAccess> implements DelegatingReadAccess {

        private final DataSpec m_spec;

        protected A m_delegateAccess;

        public AbstractDelegatingReadAccess(final DataSpec spec) {
            m_spec = spec;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void setDelegateAccess(final ReadAccess access) {
            m_delegateAccess = (A)access;
        }

        @Override
        public boolean isMissing() {
            return (m_delegateAccess == null) || m_delegateAccess.isMissing();
        }

        @Override
        public DataSpec getDataSpec() {
            return m_spec;
        }

        @Override
        public String toString() {
            return m_delegateAccess.toString();
        }
    }
}
