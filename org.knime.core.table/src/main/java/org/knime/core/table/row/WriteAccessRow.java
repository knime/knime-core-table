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
package org.knime.core.table.row;

import org.knime.core.table.access.WriteAccess;

/**
 * Represents a row of {@link WriteAccess WriteAccesses}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public interface WriteAccessRow {

    /**
     * @return the number of columns in this {@link WriteAccess}
     */
    int getNumColumns();

    /**
     * Provides write access to values within the individual columns.<br>
     * <b>NOTE:</b> Repetitive calls with the same index return the same access instance.
     *
     * @param <A> the concrete type of access
     * @param index to the column for which to get the access
     * @return the access for column
     */
    <A extends WriteAccess> A getWriteAccess(int index);

    /**
     * Copies the given row into this row.
     *
     * @param row the row to copy
     */
    void setFrom(ReadAccessRow row);
}
