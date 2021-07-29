package org.knime.core.table.schema;

import org.knime.core.table.schema.traits.DataTrait;
import org.knime.core.table.schema.traits.DataTraits;
import org.knime.core.table.schema.traits.DefaultDataTraits;
import org.knime.core.table.schema.traits.DictEncodingTrait;

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
public interface DataSpecs {

    final class DataSpecWithTraits {

        private final DataSpec spec;

        private final DataTraits traits;

        DataSpecWithTraits(final DataSpec spec, final DataTrait... traits) {
            this.spec = spec;
            this.traits = (traits.length == 0) ? DefaultDataTraits.EMPTY : new DefaultDataTraits(traits);
        }

        public DataSpec spec() {
            return spec;
        }

        public DataTraits traits() {
            return traits;
        }
    }

    DataTrait DICT_ENCODING = new DictEncodingTrait(true);

    DataSpecWithTraits BOOLEAN = new DataSpecWithTraits(BooleanDataSpec.INSTANCE);

    static DataSpecWithTraits BOOLEAN(final DataTrait... traits) {
        return new DataSpecWithTraits(BooleanDataSpec.INSTANCE, traits);
    }

    DataSpecWithTraits BYTE = new DataSpecWithTraits(ByteDataSpec.INSTANCE);

    static DataSpecWithTraits BYTE(final DataTrait... traits) {
        return new DataSpecWithTraits(ByteDataSpec.INSTANCE, traits);
    }

    DataSpecWithTraits DOUBLE = new DataSpecWithTraits(DoubleDataSpec.INSTANCE);

    static DataSpecWithTraits DOUBLE(final DataTrait... traits) {
        return new DataSpecWithTraits(DoubleDataSpec.INSTANCE, traits);
    }

    DataSpecWithTraits DURATION = new DataSpecWithTraits(DurationDataSpec.INSTANCE);

    static DataSpecWithTraits DURATION(final DataTrait... traits) {
        return new DataSpecWithTraits(DurationDataSpec.INSTANCE, traits);
    }

    DataSpecWithTraits FLOAT = new DataSpecWithTraits(FloatDataSpec.INSTANCE);

    static DataSpecWithTraits FLOAT(final DataTrait... traits) {
        return new DataSpecWithTraits(FloatDataSpec.INSTANCE, traits);
    }

    DataSpecWithTraits INT = new DataSpecWithTraits(IntDataSpec.INSTANCE);

    static DataSpecWithTraits INT(final DataTrait... traits) {
        return new DataSpecWithTraits(IntDataSpec.INSTANCE, traits);
    }

    DataSpecWithTraits LOCALDATE = new DataSpecWithTraits(LocalDateDataSpec.INSTANCE);

    static DataSpecWithTraits LOCALDATE(final DataTrait... traits) {
        return new DataSpecWithTraits(LocalDateDataSpec.INSTANCE, traits);
    }

    DataSpecWithTraits LOCALDATETIME = new DataSpecWithTraits(LocalDateTimeDataSpec.INSTANCE);

    static DataSpecWithTraits LOCALDATETIME(final DataTrait... traits) {
        return new DataSpecWithTraits(LocalDateTimeDataSpec.INSTANCE, traits);
    }

    DataSpecWithTraits LOCALTIME = new DataSpecWithTraits(LocalTimeDataSpec.INSTANCE);

    static DataSpecWithTraits LOCALTIME(final DataTrait... traits) {
        return new DataSpecWithTraits(LocalTimeDataSpec.INSTANCE, traits);
    }

    DataSpecWithTraits LONG = new DataSpecWithTraits(LongDataSpec.INSTANCE);

    static DataSpecWithTraits LONG(final DataTrait... traits) {
        return new DataSpecWithTraits(LongDataSpec.INSTANCE, traits);
    }

    DataSpecWithTraits PERIOD = new DataSpecWithTraits(PeriodDataSpec.INSTANCE);

    static DataSpecWithTraits PERIOD(final DataTrait... traits) {
        return new DataSpecWithTraits(PeriodDataSpec.INSTANCE, traits);
    }

    DataSpecWithTraits STRING = new DataSpecWithTraits(StringDataSpec.INSTANCE);

    static DataSpecWithTraits STRING(final DataTrait... traits) {
        return new DataSpecWithTraits(StringDataSpec.INSTANCE, traits);
    }

    DataSpecWithTraits VARBINARY = new DataSpecWithTraits(VarBinaryDataSpec.INSTANCE);

    static DataSpecWithTraits VARBINARY(final DataTrait... traits) {
        return new DataSpecWithTraits(VarBinaryDataSpec.INSTANCE, traits);
    }

    DataSpecWithTraits VOID = new DataSpecWithTraits(VoidDataSpec.INSTANCE);

    static DataSpecWithTraits VOID(final DataTrait... traits) {
        return new DataSpecWithTraits(VoidDataSpec.INSTANCE, traits);
    }

    DataSpecWithTraits ZONEDDATETIME = new DataSpecWithTraits(ZonedDateTimeDataSpec.INSTANCE);

    static DataSpecWithTraits ZONEDDATETIME(final DataTrait... traits) {
        return new DataSpecWithTraits(ZonedDateTimeDataSpec.INSTANCE, traits);
    }
}
