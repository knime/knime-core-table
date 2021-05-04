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

import java.io.IOException;

/**
 * A {@link LookaheadCursor} for writing data to a storage.<br>
 * Provides a {@link #finish()} method that ensures that any data that hasn't been written out, yet, is written out
 * before the instance is closed.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 * @param <A> the type of access forwarded by this forwarder
 */
public interface WriteCursor<A> extends LookaheadCursor<A> {

    /**
     * Finishes the write process by e.g. flushing data that hasn't been written out yet and closing the instance.
     * @throws IOException if closing fails
     */
    void finish() throws IOException;
}
