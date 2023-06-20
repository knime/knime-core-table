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
 *   Created on Mar 28, 2023 by Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
package org.knime.core.table.row;

import org.knime.core.table.cursor.LookaheadCursor;

/**
 * {@link LookaheadRowAccessible} whose cursors also support random access via the
 * {@link RandomAccessCursor#moveTo(long)} method.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public interface RandomRowAccessible extends LookaheadRowAccessible {

    @Override
    RandomAccessCursor<ReadAccessRow> createCursor();

    @Override
    RandomAccessCursor<ReadAccessRow> createCursor(Selection selection);

    /**
     * Cursor that provides random access via the {@link #moveTo(long)} method.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    interface RandomAccessCursor<A> extends LookaheadCursor<A> {

        void moveTo(long row);

    }
}
