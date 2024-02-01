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
package org.knime.core.table.virtual.graph.exec;

import java.io.IOException;

import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.cursor.Cursor;
import org.knime.core.table.cursor.LookaheadCursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.row.Selection;

class SequentialNodeImpSource implements SequentialNodeImp {
    private final RowAccessible accessible;

    private final int[] cols;

    private final Selection selection;

    private final ReadAccess[] outputs;

    private Cursor<ReadAccessRow> cursor;

    private LookaheadCursor<ReadAccessRow> lookahead;

    SequentialNodeImpSource(final RowAccessible accessible, final int[] cols, final long fromRow, final long toRow) {
        this.accessible = accessible;
        this.cols = cols;
        this.selection = Selection.all().retainColumns(cols).retainRows(fromRow, toRow);
        outputs = new ReadAccess[cols.length];
    }

    @Override
    public ReadAccess getOutput(final int i) {
        return outputs[i];
    }

    @Override
    public void create() {
        cursor = accessible.createCursor(selection);
        for (int i = 0; i < outputs.length; i++) {
            outputs[i] = cursor.access().getAccess(cols[i]);
        }
        if (cursor instanceof LookaheadCursor) {
            lookahead = (LookaheadCursor<ReadAccessRow>)cursor;
        }
    }

    @Override
    public boolean forward() {
        return cursor.forward();
    }

    @Override
    public boolean canForward() {
        return lookahead.canForward();
    }

    @Override
    public void close() throws IOException {
        cursor.close();
    }
}
