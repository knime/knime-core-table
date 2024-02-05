/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   Created on Jul 13, 2021 by Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
package org.knime.core.table.schema.traits;

import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.virtual.serialization.DataTraitsSerializer;

/**
 * A single piece of additional information about a {@link DataSpec}.
 *
 * To quote Bjarne Stroustrup (C++): Think of a trait as a small object whose main purpose is to carry information used
 * by another object or algorithm to determine "policy" or "implementation details".
 *
 * Note: If you add more traits, remember to implement their JSON serialization and deserialization in
 * {@link DataTraitsSerializer}.
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
         * @return the type of key (byte, int or long)
         */
        public KeyType getKeyType() {
            return m_keyType;
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

        @Override
        public String toString() {
            return m_keyType.name();
        }

        @Override
        public boolean equals(final Object obj) {
            if (this == obj) {
                return true;
            } else if (obj instanceof DictEncodingTrait) {
                return m_keyType == ((DictEncodingTrait)obj).m_keyType;
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return m_keyType.hashCode();
        }
    }

}
