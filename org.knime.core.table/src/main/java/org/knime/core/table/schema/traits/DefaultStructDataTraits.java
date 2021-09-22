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

import java.util.ArrayList;
import java.util.List;

/**
 * Special implementation of {@link DefaultDataTraits} for structs
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public class DefaultStructDataTraits extends DefaultDataTraits implements StructDataTraits {

    private final DataTraits[] m_inner;

    /**
     * Create DataTraits for a struct, without traits for the struct itself, but only traits for the contained types
     *
     * @param inner Traits for the contained types
     */
    public DefaultStructDataTraits(final DataTraits... inner) {
        super();

        if (inner == null) {
            throw new IllegalArgumentException("Inner traits should not be null");
        }
        m_inner = inner;
    }

    /**
     * Create DataTraits for a struct, with traits for the struct and traits for the contained types
     *
     * @param outer Traits for the struct itself
     * @param inner Traits for the contained types
     */
    public DefaultStructDataTraits(final DataTrait[] outer, final DataTraits... inner) {
        super(outer);

        if (inner == null) {
            throw new IllegalArgumentException("Inner traits should not be null");
        }
        m_inner = inner;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataTraits getDataTraits(final int i) {
        return m_inner[i];
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int size() {
        return m_inner.length;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private final List<DataTraits> m_innerTraits = new ArrayList<>();

        private final List<DataTrait> m_outerTraits = new ArrayList<>();

        private Builder() {

        }

        public Builder addInnerTraits(final DataTraits innerTraits) {
            m_innerTraits.add(innerTraits);
            return this;
        }

        public Builder addInnerTraits(final DataTrait... innerTraits) {
            return addInnerTraits(new DefaultDataTraits(innerTraits));
        }

        public DefaultStructDataTraits build() {
            return new DefaultStructDataTraits(m_outerTraits.toArray(DataTrait[]::new),
                m_innerTraits.toArray(DataTraits[]::new));
        }
    }

}
