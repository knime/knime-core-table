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
 *   May 4, 2021 (marcel): created
 */
package org.knime.core.table;

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
import org.knime.core.table.access.WriteAccess;
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
import org.knime.core.table.schema.VoidDataSpec;
import org.knime.core.table.schema.ZonedDateTimeDataSpec;

final class WriteAccessValueSetter {

    private static final Mapper MAPPER = new Mapper();

    public static synchronized void setValue(final DataSpec spec, final WriteAccess access, final Object value) {
        if (value == null) {
            access.setMissing();
        } else {
            MAPPER.m_access = access;
            MAPPER.m_value = value;
            spec.accept(MAPPER);
            MAPPER.m_access = null;
            MAPPER.m_value = null;
        }
    }

    private static final class Mapper implements DataSpec.Mapper<Void> {

        private WriteAccess m_access;

        private Object m_value;

        @Override
        public Void visit(final BooleanDataSpec spec) {
            ((BooleanWriteAccess)m_access).setBooleanValue((boolean)m_value);
            return null;
        }

        @Override
        public Void visit(final ByteDataSpec spec) {
            ((ByteWriteAccess)m_access).setByteValue((byte)m_value);
            return null;
        }

        @Override
        public Void visit(final DoubleDataSpec spec) {
            ((DoubleWriteAccess)m_access).setDoubleValue((double)m_value);
            return null;
        }

        @Override
        public Void visit(final DurationDataSpec spec) {
            throw new IllegalStateException("not yet implemented"); // TODO: implement
        }

        @Override
        public Void visit(final FloatDataSpec spec) {
            ((FloatWriteAccess)m_access).setFloatValue((float)m_value);
            return null;
        }

        @Override
        public Void visit(final IntDataSpec spec) {
            ((IntWriteAccess)m_access).setIntValue((int)m_value);
            return null;
        }

        @Override
        public Void visit(final LocalDateDataSpec spec) {
            throw new IllegalStateException("not yet implemented"); // TODO: implement
        }

        @Override
        public Void visit(final LocalDateTimeDataSpec spec) {
            throw new IllegalStateException("not yet implemented"); // TODO: implement
        }

        @Override
        public Void visit(final LocalTimeDataSpec spec) {
            throw new IllegalStateException("not yet implemented"); // TODO: implement
        }

        @Override
        public Void visit(final LongDataSpec spec) {
            ((LongWriteAccess)m_access).setLongValue((long)m_value);
            return null;
        }

        @Override
        public Void visit(final PeriodDataSpec spec) {
            throw new IllegalStateException("not yet implemented"); // TODO: implement
        }

        @Override
        public Void visit(final VarBinaryDataSpec spec) {
            ((VarBinaryWriteAccess)m_access).setByteArray((byte[])m_value);
            return null;
        }

        @Override
        public Void visit(final VoidDataSpec spec) {
            // Nothing to do.
            return null;
        }

        @Override
        public Void visit(final StructDataSpec spec) {
            final StructWriteAccess structAccess = (StructWriteAccess)m_access;
            final Object[] structValue = (Object[])m_value;
            for (int i = 0; i < structValue.length; i++) {
                setValue(spec.getDataSpec(i), structAccess.getWriteAccess(i), structValue[i]);
            }
            return null;
        }

        @Override
        public Void visit(final ListDataSpec spec) {
            final ListWriteAccess listAccess = (ListWriteAccess)m_access;
            final Object[] listValue = (Object[])m_value;
            listAccess.create(listValue.length);
            for (int i = 0; i < listValue.length; i++) {
                setValue(spec.getInner(), listAccess.getWriteAccess(i), listValue[i]);
            }
            return null;
        }

        @Override
        public Void visit(final ZonedDateTimeDataSpec spec) {
            throw new IllegalStateException("not yet implemented"); // TODO: implement
        }

        @Override
        public Void visit(final StringDataSpec spec) {
            ((StringWriteAccess)m_access).setStringValue((String)m_value);
            return null;
        }
    }
}
