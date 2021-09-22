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
 *   Created on Sep 24, 2021 by Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
package org.knime.core.table.schema.traits;

import java.util.Arrays;
import java.util.stream.Stream;

import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.ListDataSpec;
import org.knime.core.table.schema.StructDataSpec;

import com.google.common.base.Preconditions;

/**
 * Utility functions for dealing with {@link DataTrait} and {@link DataTraits}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class DataTraitUtils {

    /**
     * Returns a new DataTraits instance with the provided trait appended to it.
     *
     * @param traits to add a trait to
     * @param additionalTraits to add
     * @return a new {@link DataTraits} instance containing its old traits plus trait
     * @throws IllegalArgumentException if traits already contain a DataTrait of the same type as trait
     */
    public static DataTraits withTrait(final DataTraits traits, final DataTrait... additionalTraits) {
        checkThatNoneIsContained(traits, additionalTraits);
        var newTraits = addTraits(traits.getTraits(), additionalTraits);
        if (traits instanceof StructDataTraits) {
            var structTraits = (StructDataTraits)traits;
            var innerTraits = new DataTraits[structTraits.size()];
            Arrays.setAll(innerTraits, i -> structTraits.getDataTraits(i));
            return new DefaultStructDataTraits(newTraits, innerTraits);
        } else if (traits instanceof ListDataTraits) {
            var listTraits = (ListDataTraits)traits;
            return new DefaultListDataTraits(newTraits, listTraits.getInner());
        } else {
            return new DefaultDataTraits(newTraits);
        }
    }

    private static void checkThatNoneIsContained(final DataTraits traits, final DataTrait[] newTraits) {
        for (var trait : newTraits) {
            Preconditions.checkArgument(!traits.hasTrait(trait.getClass()),
                "The traits '%s' already contain a trait of type '%s' ('%s')", traits, trait.getClass(), trait);
        }
    }

    private static DataTrait[] addTraits(final DataTrait[] traits, final DataTrait[] additionalTraits) {
        return Stream.concat(//
            Stream.of(traits), //
            Stream.of(additionalTraits))//
            .toArray(DataTrait[]::new);
    }

    /**
     * Provides empty traits for the provided spec.
     *
     * @param spec of the data
     * @return an empty {@link DataTrait} that is compatible with {@link DataSpec spec}
     */
    public static DataTraits emptyTraits(final DataSpec spec) {
        if (spec instanceof StructDataSpec) {
            var structSpec = (StructDataSpec)spec;
            var innerTraits = new DataTraits[structSpec.size()];
            Arrays.setAll(innerTraits, i -> DataTraitUtils.emptyTraits(structSpec.getDataSpec(i)));
            return new DefaultStructDataTraits(innerTraits);
        } else if (spec instanceof ListDataSpec) {
            var listSpec = (ListDataSpec)spec;
            return new DefaultListDataTraits(emptyTraits(listSpec.getInner()));
        } else {
            return DefaultDataTraits.EMPTY;
        }
    }

    private DataTraitUtils() {
    }

}
