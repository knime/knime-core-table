package org.knime.core.table.schema;

import java.util.Arrays;

import org.knime.core.table.schema.traits.DataTrait;
import org.knime.core.table.schema.traits.DataTrait.DictEncodingTrait;
import org.knime.core.table.schema.traits.DataTraits;
import org.knime.core.table.schema.traits.DefaultDataTraits;
import org.knime.core.table.schema.traits.DefaultListDataTraits;
import org.knime.core.table.schema.traits.DefaultStructDataTraits;

/**
 * Defines constants and methods that each combine a {@link DataSpec} with {@link DataTrait}s.
 * This is used for constructing {@code ColumnarSchema} through {@link ColumnarSchema#of ColumnarSchema.of(...)}.
 * <p>
 * For example:
 * <pre>{@code
 * var schema1 = ColumnarSchema.of(DOUBLE, INT, STRING);
 * var schema2 = ColumnarSchema.of(INT, STRING(DICT_ENCODING), BOOLEAN);
 * var schema3 = ColumnarSchema.of(VARBINARY);
 * var schema4 = ColumnarSchema.of(VARBINARY(DICT_ENCODING));
 * }</pre>
 *
 * @author Tobias Pietzsch
 */
@SuppressWarnings("javadoc")
public final class DataSpecs {

    public static final class DataSpecWithTraits {

        private final DataSpec m_spec;

        private final DataTraits m_traits;

        public DataSpecWithTraits(final DataSpec spec, final DataTraits traits) {
            this.m_spec = spec;
            this.m_traits = traits;
        }

        DataSpecWithTraits(final DataSpec spec, final DataTrait... traits) {
            this.m_spec = spec;
            this.m_traits = (traits.length == 0) ? DefaultDataTraits.EMPTY : new DefaultDataTraits(traits);
        }

        public DataSpec spec() {
            return m_spec;
        }

        public DataTraits traits() {
            return m_traits;
        }
    }

    public static final DataTrait DICT_ENCODING = new DictEncodingTrait();

    /**
     * Activate dictionary encoding with a special key type e.g. as follows:
     * <pre>{@code
     * var schema = ColumnarSchema.of(STRING(DICT_ENCODING(KeyType.BYTE_KEY)));
     * }</pre>
     *
     * @param keyType the key type to use for dictionary encoding
     * @return The dictionary encoding trait
     */
    public static final DataTrait DICT_ENCODING(final DictEncodingTrait.KeyType keyType) {
        return new DictEncodingTrait(keyType);
    }

    public static final DataSpecWithTraits BOOLEAN = new DataSpecWithTraits(BooleanDataSpec.INSTANCE);

    public static DataSpecWithTraits BOOLEAN(final DataTrait... traits) {
        return new DataSpecWithTraits(BooleanDataSpec.INSTANCE, traits);
    }

    public static final DataSpecWithTraits BYTE = new DataSpecWithTraits(ByteDataSpec.INSTANCE);

    public static DataSpecWithTraits BYTE(final DataTrait... traits) {
        return new DataSpecWithTraits(ByteDataSpec.INSTANCE, traits);
    }

    public static final DataSpecWithTraits DOUBLE = new DataSpecWithTraits(DoubleDataSpec.INSTANCE);

    public static DataSpecWithTraits DOUBLE(final DataTrait... traits) {
        return new DataSpecWithTraits(DoubleDataSpec.INSTANCE, traits);
    }

    public static final DataSpecWithTraits DURATION = new DataSpecWithTraits(DurationDataSpec.INSTANCE);

    public static DataSpecWithTraits DURATION(final DataTrait... traits) {
        return new DataSpecWithTraits(DurationDataSpec.INSTANCE, traits);
    }

    public static final DataSpecWithTraits FLOAT = new DataSpecWithTraits(FloatDataSpec.INSTANCE);

    public static DataSpecWithTraits FLOAT(final DataTrait... traits) {
        return new DataSpecWithTraits(FloatDataSpec.INSTANCE, traits);
    }

    public static final DataSpecWithTraits INT = new DataSpecWithTraits(IntDataSpec.INSTANCE);

    public static DataSpecWithTraits INT(final DataTrait... traits) {
        return new DataSpecWithTraits(IntDataSpec.INSTANCE, traits);
    }

    public static final DataSpecWithTraits LOCALDATE = new DataSpecWithTraits(LocalDateDataSpec.INSTANCE);

    public static DataSpecWithTraits LOCALDATE(final DataTrait... traits) {
        return new DataSpecWithTraits(LocalDateDataSpec.INSTANCE, traits);
    }

    public static final DataSpecWithTraits LOCALDATETIME = new DataSpecWithTraits(LocalDateTimeDataSpec.INSTANCE);

    public static DataSpecWithTraits LOCALDATETIME(final DataTrait... traits) {
        return new DataSpecWithTraits(LocalDateTimeDataSpec.INSTANCE, traits);
    }

    public static final DataSpecWithTraits LOCALTIME = new DataSpecWithTraits(LocalTimeDataSpec.INSTANCE);

    public static DataSpecWithTraits LOCALTIME(final DataTrait... traits) {
        return new DataSpecWithTraits(LocalTimeDataSpec.INSTANCE, traits);
    }

    public static final DataSpecWithTraits LONG = new DataSpecWithTraits(LongDataSpec.INSTANCE);

    public static DataSpecWithTraits LONG(final DataTrait... traits) {
        return new DataSpecWithTraits(LongDataSpec.INSTANCE, traits);
    }

    public static final DataSpecWithTraits PERIOD = new DataSpecWithTraits(PeriodDataSpec.INSTANCE);

    public static DataSpecWithTraits PERIOD(final DataTrait... traits) {
        return new DataSpecWithTraits(PeriodDataSpec.INSTANCE, traits);
    }

    public static final DataSpecWithTraits STRING = new DataSpecWithTraits(StringDataSpec.INSTANCE);

    public static final DataSpecWithTraits STRING(final DataTrait... traits) {
        return new DataSpecWithTraits(StringDataSpec.INSTANCE, traits);
    }

    public static final DataSpecWithTraits VARBINARY = new DataSpecWithTraits(VarBinaryDataSpec.INSTANCE);

    public static final DataSpecWithTraits VARBINARY(final DataTrait... traits) {
        return new DataSpecWithTraits(VarBinaryDataSpec.INSTANCE, traits);
    }

    public static final DataSpecWithTraits VOID = new DataSpecWithTraits(VoidDataSpec.INSTANCE);

    public static DataSpecWithTraits VOID(final DataTrait... traits) {
        return new DataSpecWithTraits(VoidDataSpec.INSTANCE, traits);
    }

    public static final DataSpecWithTraits ZONEDDATETIME = new DataSpecWithTraits(ZonedDateTimeDataSpec.INSTANCE);

    public static DataSpecWithTraits ZONEDDATETIME(final DataTrait... traits) {
        return new DataSpecWithTraits(ZonedDateTimeDataSpec.INSTANCE, traits);
    }

    /**
     * Helper class to construct DataSpecWithTraits of lists.
     * In conjunction with the definition below (which can be statically imported)
     * it can be used as follows:
     *
     * <pre>{@code
     * LIST(outerTraits...).of(innerSpecWithTraits)
     * e.g.
     * LIST.of(INT)
     * LIST.of(STRING(DICT_ENCODING))
     * }</pre>
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    public static final class ListSpecWithTraitsBuilder {
        private DataTrait[] m_traits;

        private ListSpecWithTraitsBuilder(final DataTrait... traits) {
            m_traits = traits;
        }

        public DataSpecWithTraits of(final DataSpecWithTraits inner) {
            return new DataSpecWithTraits(new ListDataSpec(inner.spec()), new DefaultListDataTraits(m_traits, inner.traits()));
        }
    }

    public static final ListSpecWithTraitsBuilder LIST = new ListSpecWithTraitsBuilder();

    public static ListSpecWithTraitsBuilder LIST(final DataTrait... traits) {
        return new ListSpecWithTraitsBuilder(traits);
    }

    /**
     * Helper class to construct DataSpecWithTraits of structs.
     * In conjunction with the definition below (which can be statically imported)
     * it can be used as follows:
     *
     * <pre>{@code
     * STRUCT(outerTraits...).of(innerSpecsWithTraits...)
     * e.g.
     * STRUCT.of(INT, DOUBLE)
     * STRUCT.of(STRING(DICT_ENCODING), LOCALDATE)
     * }</pre>
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    public static final class StructSpecWithTraitsBuilder {
        private DataTrait[] m_traits;

        private StructSpecWithTraitsBuilder(final DataTrait... traits) {
            m_traits = traits;
        }

        public DataSpecWithTraits of(final DataSpecWithTraits... inner) {
            return new DataSpecWithTraits(new StructDataSpec(Arrays.stream(inner).map(DataSpecWithTraits::spec).toArray(DataSpec[]::new)),
                new DefaultStructDataTraits(m_traits, Arrays.stream(inner).map(DataSpecWithTraits::traits).toArray(DataTraits[]::new)));
        }
    }

    public static final StructSpecWithTraitsBuilder STRUCT = new StructSpecWithTraitsBuilder();

    public static final StructSpecWithTraitsBuilder STRUCT(final DataTrait... traits) {
        return new StructSpecWithTraitsBuilder(traits);
    }

    private DataSpecs() {

    }
}
