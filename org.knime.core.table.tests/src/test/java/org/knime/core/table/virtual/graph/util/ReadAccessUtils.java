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
 */
package org.knime.core.table.virtual.graph.util;

import org.knime.core.table.access.BooleanAccess;
import org.knime.core.table.access.DoubleAccess;
import org.knime.core.table.access.IntAccess;
import org.knime.core.table.access.LongAccess;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.access.StringAccess;

public class ReadAccessUtils {
    public static String toString(final ReadAccess access) {
        if (access == null) {
            return "null";
        } else if (access instanceof IntAccess.IntReadAccess) {
            final IntAccess.IntReadAccess a = (IntAccess.IntReadAccess)access;
            return "(int) " + (a.isMissing() ? "-missing-" : a.getIntValue());
        } else if (access instanceof LongAccess.LongReadAccess) {
            final LongAccess.LongReadAccess a = (LongAccess.LongReadAccess)access;
            return "(long) " + (a.isMissing() ? "-missing-" : a.getLongValue());
        } else if (access instanceof StringAccess.StringReadAccess) {
            final StringAccess.StringReadAccess a = (StringAccess.StringReadAccess)access;
            return "(string) " + (a.isMissing() ? "-missing-" : a.getStringValue());
        } else if (access instanceof DoubleAccess.DoubleReadAccess) {
            final DoubleAccess.DoubleReadAccess a = (DoubleAccess.DoubleReadAccess)access;
            return "(double) " + (a.isMissing() ? "-missing-" : a.getDoubleValue());
        } else if (access instanceof BooleanAccess.BooleanReadAccess) {
            final BooleanAccess.BooleanReadAccess a = (BooleanAccess.BooleanReadAccess)access;
            return "(bool) " + (a.isMissing() ? "-missing-" : a.getBooleanValue());
        }
        throw new UnsupportedOperationException("not implemented yet (access = " + access + ")");
    }
}
