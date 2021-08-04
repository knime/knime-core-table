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

import org.knime.core.table.schema.DataSpec;

/**
 * A single piece of additional information about a {@link DataSpec}.
 *
 * To quote Bjarne Stroustrup (C++): Think of a trait as a small object whose main purpose is to carry
 * information used by another object or algorithm to determine "policy" or "implementation details".
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public interface DataTrait {
    /**
     * If the {@link DictEncodingTrait} is provided alongside a {@link DataSpec},
     * and it is enabled, that means the data should be stored using dictionary encoding.
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    public static class DictEncodingTrait implements DataTrait {
        private final boolean m_enabled;

        /**
         * Create a dictionary encoding trait, but leave it disabled
         */
        public DictEncodingTrait() {
            this(false);
        }

        /**
         * Create a dictionary encoding trait and possibly enable it
         * @param enabled Whether dictionary encoding should be enabled
         */
        public DictEncodingTrait(final boolean enabled) {
            m_enabled = enabled;
        }

        /**
         * @return whether dictionary encoding is enabled
         */
        public boolean isEnabled() {
            return m_enabled;
        }

        /**
         * Check whether the dictionary encoding trait is enabled for
         * a given {@link DataTraits} container.
         *
         * @param traits The traits to check
         * @return true if the {@link DictEncodingTrait} is present and enabled in traits
         */
        public static boolean isEnabled(final DataTraits traits) {
            if (traits == null) {
                return false;
            }
            final DictEncodingTrait trait = traits.get(DictEncodingTrait.class);
            return trait != null && trait.isEnabled();
        }
    }

}
