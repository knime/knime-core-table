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
 *   Created on Apr 20, 2021 by Adrian Nembach
 */
package org.knime.core.table.row;

import org.knime.core.table.access.ReadAccess;

/**
 * Represents a row of {@link ReadAccess ReadAccesses}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 * @noreference This class is not intended to be referenced by clients.
 * @noimplement This class is not intended to be implemented by clients.
 */
public interface ReadAccessRow {

    /**
     * @return the number of columns
     */
    int size();

    /**
     * Provides access to values within the individual columns.<br>
     * <b>NOTE:</b> Repetitive calls with the same index return the same access instance but the instance may point to
     * different values. It is the responsibility of the caller to retrieve the value from the access if they want to
     * store it.
     *
     * @param <A> the concrete type of access
     * @param index to the column for which to get the access
     * @return the access for column
     */
    <A extends ReadAccess> A getAccess(int index);
}
