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

    /**
     * Checks if a {@link DataTrait} of the provided class is contained at some level of the provided DataTraits.
     *
     * @param traitClass to check for
     * @param traits to check in
     * @return true if a traitClass is contained somewhere in the provided traits
     */
    public static boolean containsDataTrait(final Class<? extends DataTrait> traitClass, final DataTraits traits) {
        if (traits.hasTrait(traitClass)) {
            return true;
        } else if (isStruct(traits)) {
            var structTraits = (StructDataTraits)traits;
            for (int i = 0; i < structTraits.size(); i++) {
                if (containsDataTrait(traitClass, structTraits.getDataTraits(i))) {
                    return true;
                }
            }
        } else if (isList(traits)) {
            return containsDataTrait(traitClass, ((ListDataTraits)traits).getInner());
        }
        return false;
    }

    private DataTraitUtils() {
    }

}
