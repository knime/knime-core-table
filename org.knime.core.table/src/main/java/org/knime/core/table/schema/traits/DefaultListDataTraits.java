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
 *   Created on Jul 13, 2021 by Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
package org.knime.core.table.schema.traits;

/**
 * Special implementation of {@link DefaultDataTraits} for Lists
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public class DefaultListDataTraits extends DefaultDataTraits implements ListDataTraits {

    private DataTraits m_inner;

    /**
     * Create DataTraits for a list, without traits for the list itself, but only traits for the contained type
     * @param inner Traits for the contained type
     */
    public DefaultListDataTraits(final DataTraits inner) {
        super();

        if (inner == null) {
            throw new IllegalArgumentException("Inner traits should not be null");
        }
        m_inner = inner;
    }

    /**
     * Create DataTraits for a list, with traits for the list and traits for the contained type
     * @param outer Traits for the list itself
     * @param inner Traits for the contained type, should not be null
     */
    public DefaultListDataTraits(final DataTrait[] outer, final DataTraits inner) {
        super(outer);

        if (inner == null) {
            throw new IllegalArgumentException("Inner traits should not be null");
        }
        m_inner = inner;
    }

    @Override
    public DataTraits getInner() {
        return m_inner;
    }

    @Override
    public boolean equals(final Object obj) {
        if (super.equals(obj)) {
            var other = (DefaultListDataTraits)obj;
            return m_inner.equals(other.m_inner);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + 37 * m_inner.hashCode();
    }

}
