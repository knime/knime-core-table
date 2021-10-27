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

import java.util.Arrays;

/**
 * Default implementation of {@link DataTraits}, simply holding
 * an array of traits.
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public class DefaultDataTraits implements DataTraits {

    private DataTrait[] m_traits;

    /**
     * Default instance without any traits set
     */
    public static final DefaultDataTraits EMPTY = new DefaultDataTraits();

    private DefaultDataTraits() {
        m_traits = new DataTrait[0];
    }

    /**
     * Create a DefaultDataTraits object from a list of {@link DataTrait}s
     * @param traits
     */
    public DefaultDataTraits(final DataTrait... traits) {
        m_traits = traits;
    }

    @Override
    public <T extends DataTrait> T get(final Class<T> type) {
        for (var t : m_traits) {
            if (type.isInstance(t)) {
                return ( T ) t;
            }
        }

        return null;
    }

    @Override
    public DataTrait[] getTraits() {
        return m_traits.clone();
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }

        if (obj == null) {
            return false;
        }

        if (getClass().equals(obj.getClass())) {
            var other = (DefaultDataTraits)obj;
            return Arrays.equals(m_traits, other.m_traits);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(m_traits);
    }

    @Override
    public String toString() {
        return Arrays.toString(m_traits);
    }

}
