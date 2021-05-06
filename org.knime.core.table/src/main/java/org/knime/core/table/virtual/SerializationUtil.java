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
 *   Apr 14, 2021 (marcel): created
 */
package org.knime.core.table.virtual;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UncheckedIOException;

import org.knime.core.table.schema.BooleanDataSpec;
import org.knime.core.table.schema.ByteDataSpec;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.DoubleDataSpec;
import org.knime.core.table.schema.DurationDataSpec;
import org.knime.core.table.schema.FloatDataSpec;
import org.knime.core.table.schema.IntDataSpec;
import org.knime.core.table.schema.ListDataSpec;
import org.knime.core.table.schema.LocalDateDataSpec;
import org.knime.core.table.schema.LocalDateTimeDataSpec;
import org.knime.core.table.schema.LocalTimeDataSpec;
import org.knime.core.table.schema.LongDataSpec;
import org.knime.core.table.schema.PeriodDataSpec;
import org.knime.core.table.schema.StringDataSpec;
import org.knime.core.table.schema.StructDataSpec;
import org.knime.core.table.schema.VarBinaryDataSpec;
import org.knime.core.table.schema.VoidDataSpec;
import org.knime.core.table.schema.ZonedDateTimeDataSpec;

// TODO: move to specialized DataInput/DataOutput implementations that are passed to our serializers instead?
final class SerializationUtil {

    private SerializationUtil() {}

    public static void writeIntArray(final int[] array, final DataOutput output) throws IOException {
        output.writeInt(array.length);
        for (final int entry : array) {
            output.writeInt(entry);
        }
    }

    public static int[] readIntArray(final DataInput input) throws IOException {
        final int[] array = new int[input.readInt()];
        for (int i = 0; i < array.length; i++) {
            array[i] = input.readInt();
        }
        return array;
    }

    // TODO: make these serialization identifiers (ints) more first class/part of the format. Python will require them, too...
    public static void writeDataSpec(final DataSpec spec, final DataOutput output) throws IOException {
        try {
            spec.accept(new DataSpec.Mapper<Void>() {

                @Override
                public Void visit(final BooleanDataSpec spec) {
                    return writeType(0);
                }

                @Override
                public Void visit(final ByteDataSpec spec) {
                    return writeType(1);
                }

                @Override
                public Void visit(final DoubleDataSpec spec) {
                    return writeType(2);
                }

                @Override
                public Void visit(final DurationDataSpec spec) {
                    return writeType(3);
                }

                @Override
                public Void visit(final FloatDataSpec spec) {
                    return writeType(4);
                }

                @Override
                public Void visit(final IntDataSpec spec) {
                    return writeType(5);
                }

                @Override
                public Void visit(final LocalDateDataSpec spec) {
                    return writeType(6);
                }

                @Override
                public Void visit(final LocalDateTimeDataSpec spec) {
                    return writeType(7);
                }

                @Override
                public Void visit(final LocalTimeDataSpec spec) {
                    return writeType(8);
                }

                @Override
                public Void visit(final LongDataSpec spec) {
                    return writeType(9);
                }

                @Override
                public Void visit(final PeriodDataSpec spec) {
                    return writeType(10);
                }

                @Override
                public Void visit(final VarBinaryDataSpec spec) {
                    return writeType(11);
                }

                @Override
                public Void visit(final VoidDataSpec spec) {
                    return writeType(12);
                }

                @Override
                public Void visit(final StructDataSpec spec) {
                    writeType(13);
                    try {
                        final DataSpec[] innerSpecs = spec.getInner();
                        output.writeInt(innerSpecs.length);
                        for (final DataSpec inner : innerSpecs) {
                            writeDataSpec(inner, output);
                        }
                    } catch (final IOException ex) {
                        throw new UncheckedIOException(ex);
                    }
                    return null;
                }

                @Override
                public Void visit(final ListDataSpec listDataSpec) {
                    writeType(14);
                    try {
                        writeDataSpec(listDataSpec.getInner(), output);
                    } catch (final IOException ex) {
                        throw new UncheckedIOException(ex);
                    }
                    return null;
                }

                @Override
                public Void visit(final ZonedDateTimeDataSpec spec) {
                    return writeType(15);
                }

                @Override
                public Void visit(final StringDataSpec spec) {
                    return writeType(16);
                }

                private Void writeType(final int dataSpecType) {
                    try {
                        output.writeInt(dataSpecType);
                    } catch (final IOException ex) {
                        throw new UncheckedIOException(ex);
                    }
                    return null;
                }
            });
        } catch (final UncheckedIOException ex) {
            throw ex.getCause();
        }
    }

    public static DataSpec readDataSpec(final DataInput input) throws IOException {
        final int type = input.readInt();
        switch (type) {
            case 0:
                return BooleanDataSpec.INSTANCE;
            case 1:
                return ByteDataSpec.INSTANCE;
            case 2:
                return DoubleDataSpec.INSTANCE;
            case 3:
                return DurationDataSpec.INSTANCE;
            case 4:
                return FloatDataSpec.INSTANCE;
            case 5:
                return IntDataSpec.INSTANCE;
            case 6:
                return LocalDateDataSpec.INSTANCE;
            case 7:
                return LocalDateTimeDataSpec.INSTANCE;
            case 8:
                return LocalTimeDataSpec.INSTANCE;
            case 9:
                return LongDataSpec.INSTANCE;
            case 10:
                return PeriodDataSpec.INSTANCE;
            case 11:
                return VarBinaryDataSpec.INSTANCE;
            case 12:
                return VoidDataSpec.INSTANCE;
            case 13:
                final int numInnerSpecs = input.readInt();
                final DataSpec[] innerSpecs = new DataSpec[numInnerSpecs];
                for (int i = 0; i < innerSpecs.length; i++) {
                    innerSpecs[i] = readDataSpec(input);
                }
                return new StructDataSpec(innerSpecs);
            case 14:
                final DataSpec innerSpec = readDataSpec(input);
                return new ListDataSpec(innerSpec);
            case 15:
                return ZonedDateTimeDataSpec.INSTANCE;
            case 16:
                return StringDataSpec.INSTANCE;
            default:
                throw new IllegalStateException("Unknown data spec type identifier: " + type);
        }
    }
}
