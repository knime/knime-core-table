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
 */
package org.knime.core.table.schema;

import java.util.Arrays;

import org.knime.core.table.schema.traits.DataTrait;
import org.knime.core.table.schema.traits.DataTrait.DictEncodingTrait;
import org.knime.core.table.schema.traits.DataTraits;
import org.knime.core.table.schema.traits.DefaultDataTraits;
import org.knime.core.table.schema.traits.DefaultListDataTraits;
import org.knime.core.table.schema.traits.DefaultStructDataTraits;
import org.knime.core.table.schema.traits.LogicalTypeTrait;

/**
 * Defines constants and methods that each combine a {@link DataSpec} with {@link DataTrait}s. This is used for
 * constructing {@code ColumnarSchema} through {@link ColumnarSchema#of ColumnarSchema.of(...)}.
 * <p>
 * For example:
 *
 * <pre>
 * {
 *     &#64;code
 *     var schema1 = ColumnarSchema.of(DOUBLE, INT, STRING);
 *     var schema2 = ColumnarSchema.of(INT, STRING(DICT_ENCODING), BOOLEAN);
 *     var schema3 = ColumnarSchema.of(VARBINARY);
 *     var schema4 = ColumnarSchema.of(VARBINARY(DICT_ENCODING));
 * }
 * </pre>
 *
 * @author Tobias Pietzsch
 */
@SuppressWarnings("javadoc")
public final class DataSpecs {

    public record DataSpecWithTraits(DataSpec spec, DataTraits traits) {

        DataSpecWithTraits(final DataSpec spec, final DataTrait... traits) {
            this(spec, (traits.length == 0) ? DefaultDataTraits.EMPTY : new DefaultDataTraits(traits));
        }

    }

    public static final DataTrait DICT_ENCODING = new DictEncodingTrait();

    /**
     * Activate dictionary encoding with a special key type e.g. as follows:
     *
     * <pre>
     * {
     *     &#64;code
     *     var schema = ColumnarSchema.of(STRING(DICT_ENCODING(KeyType.BYTE_KEY)));
     * }
     * </pre>
     *
     * @param keyType the key type to use for dictionary encoding
     * @return The dictionary encoding trait
     */
    public static final DataTrait DICT_ENCODING(final DictEncodingTrait.KeyType keyType) { // NOSONAR: we provide TYPE and TYPE(TRAITS...) as overloads
        return new DictEncodingTrait(keyType);
    }

    /**
     * Construct a {@link LogicalTypeTrait} from a string.
     *
     * <pre>
     * {
     *     &#64;code
     *     var schema = ColumnarSchema.of(LONG(LOGICAL_TYPE("UUID")));
     * }
     * </pre>
     *
     * @param logicalType The logical type identifier
     * @return The logical type trait
     */
    public static final DataTrait LOGICAL_TYPE(final String logicalType) { // NOSONAR: we provide TYPE and TYPE(TRAITS...) as overloads
        return new LogicalTypeTrait(logicalType);
    }

    public static final DataSpecWithTraits BOOLEAN = new DataSpecWithTraits(BooleanDataSpec.INSTANCE);

    public static DataSpecWithTraits BOOLEAN(final DataTrait... traits) { // NOSONAR: we provide TYPE and TYPE(TRAITS...) as overloads
        return new DataSpecWithTraits(BooleanDataSpec.INSTANCE, traits);
    }

    public static final DataSpecWithTraits BYTE = new DataSpecWithTraits(ByteDataSpec.INSTANCE);

    public static DataSpecWithTraits BYTE(final DataTrait... traits) {// NOSONAR: we provide TYPE and TYPE(TRAITS...) as overloads
        return new DataSpecWithTraits(ByteDataSpec.INSTANCE, traits);
    }

    public static final DataSpecWithTraits DOUBLE = new DataSpecWithTraits(DoubleDataSpec.INSTANCE);

    public static DataSpecWithTraits DOUBLE(final DataTrait... traits) {// NOSONAR: we provide TYPE and TYPE(TRAITS...) as overloads
        return new DataSpecWithTraits(DoubleDataSpec.INSTANCE, traits);
    }

    public static final DataSpecWithTraits FLOAT = new DataSpecWithTraits(FloatDataSpec.INSTANCE);

    public static DataSpecWithTraits FLOAT(final DataTrait... traits) {// NOSONAR: we provide TYPE and TYPE(TRAITS...) as overloads
        return new DataSpecWithTraits(FloatDataSpec.INSTANCE, traits);
    }

    public static final DataSpecWithTraits INT = new DataSpecWithTraits(IntDataSpec.INSTANCE);

    public static DataSpecWithTraits INT(final DataTrait... traits) {// NOSONAR: we provide TYPE and TYPE(TRAITS...) as overloads
        return new DataSpecWithTraits(IntDataSpec.INSTANCE, traits);
    }

    public static final DataSpecWithTraits LONG = new DataSpecWithTraits(LongDataSpec.INSTANCE);

    public static DataSpecWithTraits LONG(final DataTrait... traits) {// NOSONAR: we provide TYPE and TYPE(TRAITS...) as overloads
        return new DataSpecWithTraits(LongDataSpec.INSTANCE, traits);
    }

    public static final DataSpecWithTraits STRING = new DataSpecWithTraits(StringDataSpec.INSTANCE);

    public static final DataSpecWithTraits STRING(final DataTrait... traits) {// NOSONAR: we provide TYPE and TYPE(TRAITS...) as overloads
        return new DataSpecWithTraits(StringDataSpec.INSTANCE, traits);
    }

    public static final DataSpecWithTraits VARBINARY = new DataSpecWithTraits(VarBinaryDataSpec.INSTANCE);

    public static final DataSpecWithTraits VARBINARY(final DataTrait... traits) {// NOSONAR: we provide TYPE and TYPE(TRAITS...) as overloads
        return new DataSpecWithTraits(VarBinaryDataSpec.INSTANCE, traits);
    }

    public static final DataSpecWithTraits VOID = new DataSpecWithTraits(VoidDataSpec.INSTANCE);

    public static DataSpecWithTraits VOID(final DataTrait... traits) {// NOSONAR: we provide TYPE and TYPE(TRAITS...) as overloads
        return new DataSpecWithTraits(VoidDataSpec.INSTANCE, traits);
    }


    /**
     * Helper class to construct DataSpecWithTraits of lists. In conjunction with the definition below (which can be
     * statically imported) it can be used as follows:
     *
     * <pre>
     * {@code
     * LIST(outerTraits...).of(innerSpecWithTraits)
     * e.g.
     * LIST.of(INT)
     * LIST.of(STRING(DICT_ENCODING))
     * }
     * </pre>
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    public static final class ListSpecWithTraitsBuilder {
        private DataTrait[] m_traits;

        private ListSpecWithTraitsBuilder(final DataTrait... traits) {
            m_traits = traits;
        }

        public DataSpecWithTraits of(final DataSpecWithTraits inner) {
            return new DataSpecWithTraits(new ListDataSpec(inner.spec()),
                new DefaultListDataTraits(m_traits, inner.traits()));
        }
    }

    public static final ListSpecWithTraitsBuilder LIST = new ListSpecWithTraitsBuilder();

    public static ListSpecWithTraitsBuilder LIST(final DataTrait... traits) {// NOSONAR: we provide TYPE and TYPE(TRAITS...) as overloads
        return new ListSpecWithTraitsBuilder(traits);
    }

    /**
     * Helper class to construct DataSpecWithTraits of structs. In conjunction with the definition below (which can be
     * statically imported) it can be used as follows:
     *
     * <pre>
     * {@code
     * STRUCT(outerTraits...).of(innerSpecsWithTraits...)
     * e.g.
     * STRUCT.of(INT, DOUBLE)
     * STRUCT.of(STRING(DICT_ENCODING), LOCALDATE)
     * }
     * </pre>
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    public static final class StructSpecWithTraitsBuilder {
        private DataTrait[] m_traits;

        private StructSpecWithTraitsBuilder(final DataTrait... traits) {
            m_traits = traits;
        }

        public DataSpecWithTraits of(final DataSpecWithTraits... inner) {
            return new DataSpecWithTraits(
                new StructDataSpec(Arrays.stream(inner).map(DataSpecWithTraits::spec).toArray(DataSpec[]::new)),
                new DefaultStructDataTraits(m_traits,
                    Arrays.stream(inner).map(DataSpecWithTraits::traits).toArray(DataTraits[]::new)));
        }
    }

    public static final StructSpecWithTraitsBuilder STRUCT = new StructSpecWithTraitsBuilder();

    public static final StructSpecWithTraitsBuilder STRUCT(final DataTrait... traits) {// NOSONAR: we provide TYPE and TYPE(TRAITS...) as overloads
        return new StructSpecWithTraitsBuilder(traits);
    }

    private DataSpecs() {

    }
}
