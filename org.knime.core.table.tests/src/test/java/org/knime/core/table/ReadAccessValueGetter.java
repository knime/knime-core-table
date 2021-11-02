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
 *   Apr 28, 2021 (marcel): created
 */
package org.knime.core.table;

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
import org.knime.core.table.schema.FloatDataSpec;
import org.knime.core.table.schema.IntDataSpec;
import org.knime.core.table.schema.ListDataSpec;
import org.knime.core.table.schema.LongDataSpec;
import org.knime.core.table.schema.StringDataSpec;
import org.knime.core.table.schema.StructDataSpec;
import org.knime.core.table.schema.VarBinaryDataSpec;
import org.knime.core.table.schema.VoidDataSpec;

final class ReadAccessValueGetter {

    private static final Mapper MAPPER = new Mapper();

    public static synchronized <T> T getValue(final DataSpec spec, final ReadAccess access) {
        if (access.isMissing()) {
            return null;
        } else {
            MAPPER.m_access = access;
            @SuppressWarnings("unchecked") // Ensuring type safety is the client's responsibility.
            final T casted = (T)spec.accept(MAPPER);
            MAPPER.m_access = null;
            return casted;
        }
    }

    private ReadAccessValueGetter() {}

    private static final class Mapper implements DataSpec.Mapper<Object> {

        private ReadAccess m_access;

        @Override
        public Object visit(final BooleanDataSpec spec) {
            return ((BooleanReadAccess)m_access).getBooleanValue();
        }

        @Override
        public Object visit(final ByteDataSpec spec) {
            return ((ByteReadAccess)m_access).getByteValue();
        }

        @Override
        public Object visit(final DoubleDataSpec spec) {
            return ((DoubleReadAccess)m_access).getDoubleValue();
        }

        @Override
        public Object visit(final FloatDataSpec spec) {
            return ((FloatReadAccess)m_access).getFloatValue();
        }

        @Override
        public Object visit(final IntDataSpec spec) {
            return ((IntReadAccess)m_access).getIntValue();
        }

        @Override
        public Object visit(final LongDataSpec spec) {
            return ((LongReadAccess)m_access).getLongValue();
        }

        @Override
        public Object visit(final VarBinaryDataSpec spec) {
            return ((VarBinaryReadAccess)m_access).getByteArray();
        }

        @Override
        public Object visit(final VoidDataSpec spec) {
            return null;
        }

        @Override
        public Object visit(final StructDataSpec spec) {
            final StructReadAccess structAccess = (StructReadAccess)m_access;
            final Object[] structValue = new Object[structAccess.size()];
            for (int i = 0; i < structValue.length; i++) {
                structValue[i] = getValue(spec.getDataSpec(i), structAccess.getAccess(i));
            }
            return structValue;
        }

        @Override
        public Object visit(final ListDataSpec listDataSpec) {
            final ListReadAccess listAccess = (ListReadAccess)m_access;
            final Object[] listValue = new Object[listAccess.size()];
            for (int i = 0; i < listValue.length; i++) {
                listValue[i] = getValue(listDataSpec.getInner(), listAccess.getAccess(i));
            }
            return listValue;
        }

        @Override
        public Object visit(final StringDataSpec spec) {
            return ((StringReadAccess)m_access).getStringValue();
        }
    }
}