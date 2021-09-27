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
 * To quote Bjarne Stroustrup (C++): Think of a trait as a small object whose main purpose is to carry information used
 * by another object or algorithm to determine "policy" or "implementation details".
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public interface DataTrait {
    /**
     * If the {@link DictEncodingTrait} is provided alongside a {@link DataSpec}, and it is enabled, that means the data
     * should be stored using dictionary encoding.
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    public static final class DictEncodingTrait implements DataTrait {

        /**
         * Type of key to use for the dictionary
         */
        public enum KeyType {
                /** Use Byte keys -> max 128 different dictionary entries */
                BYTE_KEY,

                /** Use Integer keys -> max 2147483647 different entries */
                INT_KEY,

                /** Use Long keys, default */
                LONG_KEY;
        }

        private final KeyType m_keyType;

        /**
         * Create a dictionary encoding trait with default key type LONG_KEY
         */
        public DictEncodingTrait() {
            this(KeyType.LONG_KEY);
        }

        /**
         * Create a dictionary encoding trait using the provided key type
         *
         * @param keyType Which key type to use
         */
        public DictEncodingTrait(final KeyType keyType) {
            m_keyType = keyType;
        }

        /**
         * Check whether the dictionary encoding trait is present in a given {@link DataTraits} container.
         *
         * @param traits The traits to check
         * @return true if the {@link DictEncodingTrait} is present
         */
        public static boolean isEnabled(final DataTraits traits) {
            return DataTraits.hasTrait(traits, DictEncodingTrait.class);
        }

        /**
         * Return the {@link KeyType} to use for dictionary encoding if the dictionary encoding trait is present.
         * Otherwise returns null.
         *
         * @param traits The traits to check
         * @return The {@link KeyType} to use if the {@link DictEncodingTrait} is present, otherwise null.
         */
        public static KeyType keyType(final DataTraits traits) {
            if (traits == null) {
                return null;
            }
            final DictEncodingTrait trait = traits.get(DictEncodingTrait.class);
            if (trait == null) {
                return null;
            }

            return trait.m_keyType;
        }
    }

}
