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
 *   Apr 23, 2021 (marcel): created
 */
package org.knime.core.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.DoubleDataSpec;
import org.knime.core.table.schema.IntDataSpec;
import org.knime.core.table.schema.ListDataSpec;
import org.knime.core.table.schema.StringDataSpec;
import org.knime.core.table.schema.StructDataSpec;
import org.knime.core.table.schema.traits.DataTrait;
import org.knime.core.table.schema.traits.DataTraits;
import org.knime.core.table.schema.traits.DefaultDataTraits;
import org.knime.core.table.schema.traits.DefaultListDataTraits;
import org.knime.core.table.schema.traits.DefaultStructDataTraits;
import org.knime.core.table.virtual.serialization.TableTransformSpecSerializer;
import org.knime.core.table.virtual.spec.AppendMissingValuesTransformSpec;
import org.knime.core.table.virtual.spec.AppendMissingValuesTransformSpec.AppendMissingValuesTransformSpecSerializer;
import org.knime.core.table.virtual.spec.AppendTransformSpec;
import org.knime.core.table.virtual.spec.AppendTransformSpec.AppendTransformSpecSerializer;
import org.knime.core.table.virtual.spec.ColumnFilterTransformSpec;
import org.knime.core.table.virtual.spec.ColumnFilterTransformSpec.ColumnFilterTransformSpecSerializer;
import org.knime.core.table.virtual.spec.ConcatenateTransformSpec;
import org.knime.core.table.virtual.spec.ConcatenateTransformSpec.ConcatenateTransformSpecSerializer;
import org.knime.core.table.virtual.spec.PermuteTransformSpec;
import org.knime.core.table.virtual.spec.PermuteTransformSpec.PermuteTransformSpecSerializer;
import org.knime.core.table.virtual.spec.SliceTransformSpec;
import org.knime.core.table.virtual.spec.SliceTransformSpec.SliceTransformSpecSerializer;
import org.knime.core.table.virtual.spec.TableTransformSpec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.primitives.Ints;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("javadoc")
@RunWith(Parameterized.class)
public final class TableTransformSpecSerializerTest<T extends TableTransformSpec> {

    private static final Random RANDOM = new Random(1234567890);

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> createTableTransformSpecsToTest() {
        final List<Object[]> params = new ArrayList<>();

        // Append
        add(params, new AppendTransformSpec(), new AppendTransformSpecSerializer());

        // Append missing value
        final var specs = new DataSpec[] {//
            DoubleDataSpec.INSTANCE, //
            IntDataSpec.INSTANCE, //
            new ListDataSpec(DoubleDataSpec.INSTANCE), //
            StringDataSpec.INSTANCE, //
            new StructDataSpec(DoubleDataSpec.INSTANCE, IntDataSpec.INSTANCE, StringDataSpec.INSTANCE) //
        };

        final var traits = new DataTraits[] {//
            DefaultDataTraits.EMPTY, //
            DefaultDataTraits.EMPTY, //
            new DefaultListDataTraits(new DataTrait[0], DefaultDataTraits.EMPTY), //
            DefaultDataTraits.EMPTY, //
            new DefaultStructDataTraits(new DataTrait[0], DefaultDataTraits.EMPTY, DefaultDataTraits.EMPTY, DefaultDataTraits.EMPTY) //
        };


        add(params, //
            new AppendMissingValuesTransformSpec( //
                specs,
                traits
            ), //
            new AppendMissingValuesTransformSpecSerializer());

        // Column filter
        add(params, //
            new ColumnFilterTransformSpec(RANDOM.ints(100, 0, Integer.MAX_VALUE).sorted().toArray()),
            new ColumnFilterTransformSpecSerializer());

        // Concatenate
        add(params, new ConcatenateTransformSpec(), new ConcatenateTransformSpecSerializer());

        // Permute
        final List<Integer> permutation = IntStream.range(0, 100).boxed().collect(Collectors.toList());
        Collections.shuffle(permutation, RANDOM);
        add(params, //
            new PermuteTransformSpec(Ints.toArray(permutation)), new PermuteTransformSpecSerializer());

        // Slice
        final long from = RANDOM.longs(1, 0, Long.MAX_VALUE).findFirst().getAsLong();
        final long to = RANDOM.longs(1, from, Long.MAX_VALUE).findFirst().getAsLong();
        add(params, new SliceTransformSpec(from, to), new SliceTransformSpecSerializer());

        return params;
    }

    private static <T extends TableTransformSpec> void add(final List<Object[]> params, final T spec,
        final TableTransformSpecSerializer<T> serializer) {
        params.add(new Object[]{spec, serializer});
    }

    @Parameterized.Parameter(0)
    public T m_spec;

    @Parameterized.Parameter(1)
    public TableTransformSpecSerializer<T> m_serializer;

    @Test
    public void testTableTransformSpecSerializationRoundtrip() {
        final JsonNode config = m_serializer.save(m_spec, JsonNodeFactory.instance);
        final T deserialized = m_serializer.load(config);
        assertNotSame(m_spec, deserialized); // Might need to be removed in case we turn stateless specs into singletons
        assertEquals(m_spec, deserialized);
    }
}
