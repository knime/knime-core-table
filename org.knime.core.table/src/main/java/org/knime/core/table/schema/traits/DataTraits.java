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
