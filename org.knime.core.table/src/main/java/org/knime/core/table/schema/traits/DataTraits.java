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

import java.util.Optional;

import org.knime.core.table.schema.DataSpec;

/**
 * A {@link DataTraits} container holds additional information about the data stored in a column,
 * complementary to its {@link DataSpec}.
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public interface DataTraits {

    /**
     * Get the {@link DataTrait} of the given type if available, returns null otherwise.
     * @param type The {@link DataTrait} subclass to query
     * @return The trait or null
     */
    <T extends DataTrait> T get(Class<T> type);

    /**
     * @return the contained traits (never null)
     */
    DataTrait[] getTraits();


    /**
     * Indicates whether a trait is contained in this instance.
     *
     * @param <T> the type of trait
     * @param traitClass the class of trait
     * @return true if a trait of the provided class is contained in this instance
     */
    default <T extends DataTrait> boolean hasTrait(final Class<T> traitClass) {
        return get(traitClass) != null;
    }

    // TODO traits shouldn't be null in which case we can get rid of the null-safe static methods
    public static <T extends DataTrait> boolean hasTrait(final DataTraits traits, final Class<T> traitClass) {
        if (traits == null) {
            return false;
        } else {
            return traits.hasTrait(traitClass);
        }
    }

    public static <T extends DataTrait> Optional<T> getTrait(final DataTraits traits, final Class<T> traitClass) {
        if (traits == null) {
            return Optional.empty();
        } else {
            return Optional.ofNullable(traits.get(traitClass));
        }
    }
}
