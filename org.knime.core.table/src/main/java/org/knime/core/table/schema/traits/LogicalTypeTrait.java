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
 *   Created on Sep 23, 2021 by Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
package org.knime.core.table.schema.traits;

/**
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class LogicalTypeTrait implements DataTrait {

    private final String m_logicalType;

    /**
     * Constructor.
     *
     * @param logicalType the logical type of the data
     */
    public LogicalTypeTrait(final String logicalType) {
        m_logicalType = logicalType;
    }

    /**
     * @return the logical type of the data
     */
    public String getLogicalType() {
        return m_logicalType;
    }

    /**
     * @param traits to check for logical types (can be null)
     * @return true if the traits contains
     */
    public static boolean hasLogicalType(final DataTraits traits) {
        return DataTraits.hasTrait(traits, LogicalTypeTrait.class);
    }

}
