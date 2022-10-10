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
 *   Created on Apr 22, 2021 by Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
package org.knime.core.table.cursor;

import java.io.Closeable;

/**
 * Allows to iterate over a data source. Access to the underlying data is provided via the {@link Cursor#access()}
 * method. Accesses are mutable, i.e. every call to {@link #forward()} will alter the values the access provides.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 * @param <A> The type of access that is forwarded by this cursor
 *
 * @noreference This interface is not intended to be referenced by clients.
 * @noimplement This interface is not intended to be implemented by clients.
 */
public interface Cursor<A> extends Closeable {

    /**
     * Always returns the same access instance.<br>
     * This method can be called before the first call to {@link #forward()} e.g. to set up a decorator.<br>
     * However, the access won't point to any values and it is only save to access values after the first
     * {@link #forward()} call.
     *
     * @return the access that is forwarded by this cursor
     */
    A access();

    // TODO we need a special kind of access that is closeable in order to release
    // any resources (e.g. ArrowVectors) the access points to
    default A pinAccess() {
        // TODO default implementation based on BufferedAccesses
        throw new UnsupportedOperationException();
    }

    /**
     * @return true if forwarding was successful, false otherwise (i.e. at the end)
     */
    boolean forward();

}
