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
 *   Created on Apr 23, 2021 by Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
package org.knime.core.table.cursor;

import java.io.Flushable;
import java.io.IOException;

/**
 * A {@link Cursor} for writing data to a storage.<br>
 * Provides a {@link #flush()} method that ensures that any data that hasn't been written out, yet, is written out.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 * @param <A> the type of access forwarded by this forwarder
 */
public interface WriteCursor<A> extends Cursor<A>, Flushable {

    /**
     * Finishes the write process by e.g. flushing data that hasn't been written out yet.
     * Does not close the {@link WriteCursor}.
     *
     * @throws IOException if flushing fails
     */
    @Override
    void flush() throws IOException;
}
