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
     * Merges the provided {@link DataTraits} objects.
     *
     * @param traits reference
     * @param otherTraits additional
     * @return the merged {@link DataTraits}
     * @throws IllegalArgumentException if the traits are incompatible in structure or there are conflicts (i.e. the
     *             same trait contained multiple times)
     */
    public static DataTraits merge(final DataTraits traits, final DataTraits otherTraits) {
        if (isStruct(traits)) {
            Preconditions.checkArgument(isStruct(otherTraits),
                "StructDataTraits can only be merged with other StructDataTraits.");
            return mergeStruct((StructDataTraits)traits, (StructDataTraits)otherTraits);
        } else if (isList(traits)) {
            Preconditions.checkArgument(isList(otherTraits),
                "ListDataTraits can only be merged with other ListDataTraits.");
            return mergeList((ListDataTraits)traits, (ListDataTraits)otherTraits);
        } else {
            Preconditions.checkArgument(!isNested(otherTraits), "Normal traits can't be merged with nested traits.");
            return new DefaultDataTraits(addTraits(traits.getTraits(), otherTraits.getTraits()));
        }
    }

    private static ListDataTraits mergeList(final ListDataTraits traits, final ListDataTraits other) {
        checkThatNoneIsContained(traits, other.getTraits());
        var mergedInnerTraits = merge(traits.getInner(), other.getInner());
        return new DefaultListDataTraits(addTraits(traits.getTraits(), other.getTraits()), mergedInnerTraits);
    }

    private static StructDataTraits mergeStruct(final StructDataTraits traits, final StructDataTraits other) {
        checkThatNoneIsContained(traits, other.getTraits());
        Preconditions.checkArgument(traits.size() == other.size(),
            "The number of inner traits must match.");
        var mergedInnerTraits = new DataTraits[traits.size()];
        Arrays.setAll(mergedInnerTraits, i -> merge(traits.getDataTraits(i), other.getDataTraits(i)));
        return new DefaultStructDataTraits(addTraits(traits.getTraits(), other.getTraits()), mergedInnerTraits);
    }

    private static boolean isNested(final DataTraits traits) {
        return isStruct(traits) || isList(traits);
    }

    /**
     * Convenience method for checking if some DataTraits are StructDataTraits.
     *
     * @param traits to check
     * @return true if the traits are StructDataTraits
     */
    public static boolean isStruct(final DataTraits traits) {
        return traits instanceof StructDataTraits;
    }

    /**
     * Convenience method for checking if some DataTraits are ListDataTraits.
     *
     * @param traits to check
     * @return true if the traits are ListDataTraits
     */
    public static boolean isList(final DataTraits traits) {
        return traits instanceof ListDataTraits;
    }

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
        if (isStruct(traits)) {
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
