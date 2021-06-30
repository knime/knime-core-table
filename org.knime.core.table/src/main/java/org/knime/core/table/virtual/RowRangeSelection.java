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
 *   Created on May 21, 2021 by dietzc
 */
package org.knime.core.table.virtual;

/**
 * Defines a row range selection
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public interface RowRangeSelection {

    /**
     * @return start of row range (inclusive)
     */
    long fromIndex();

    /**
     * @return end of row range (exclusive)
     */
    long toIndex();
}
