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
 *   Created on Oct 22, 2021 by eric.axt
 */
package org.knime.core.table.virtual;

import org.knime.core.table.cursor.LookaheadCursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.row.Selection;

/**
 * A {@link RowAccessible} that creates {@link LookaheadCursor LookaheadCursors}.
 *
 * @author Eric Axt, KNIME GmbH, Konstanz, Germany
 */
public interface LookaheadRowAccessible extends RowAccessible {

    @Override
    LookaheadCursor<ReadAccessRow> createCursor();

    @Override
    LookaheadCursor<ReadAccessRow> createCursor(Selection selection);
}
