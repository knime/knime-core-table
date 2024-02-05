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
 *   Created on Oct 28, 2021 by Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
package org.knime.core.table.access;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.junit.Test;
import org.knime.core.table.access.ListAccess.ListReadAccess;
import org.knime.core.table.access.ListAccess.ListWriteAccess;
import org.knime.core.table.access.StringAccess.StringReadAccess;
import org.knime.core.table.access.StringAccess.StringWriteAccess;
import org.knime.core.table.access.VarBinaryAccess.VarBinaryReadAccess;
import org.knime.core.table.access.VarBinaryAccess.VarBinaryWriteAccess;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.ListDataSpec;
import org.knime.core.table.schema.VarBinaryDataSpec;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectDeserializer;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectSerializer;

/**
 * Contains unit tests for the {@link BufferedAccesses}.
 *
 * TODO complete coverage
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("javadoc")
public class BufferedAccessesTest {

    @Test
    public void testBufferedListAccess() {
        var spec = new ListDataSpec(DataSpec.stringSpec());
        var bufferedList = BufferedAccesses.createBufferedAccess(spec);
        assertThat(bufferedList.getDataSpec()).isEqualTo(spec);
        var readList = (ListReadAccess)bufferedList;
        assertThat(readList.isMissing()).isTrue();
        assertThat(readList.size()).isEqualTo(0);

        var writeList = (ListWriteAccess)bufferedList;
        writeList.create(3);
        assertThat(readList.size()).isEqualTo(3);
        assertThat(readList.isMissing()).isFalse();
        assertThat(readList.isMissing(0)).isTrue();
        writeList.setWriteIndex(0);
        StringWriteAccess writeAccess = writeList.getWriteAccess();
        writeAccess.setStringValue("foo");
        assertThat(readList.isMissing(0)).isFalse();
        readList.setIndex(0);
        assertThat(((StringReadAccess)readList.getAccess()).getStringValue()).isEqualTo("foo");
        assertThat(readList.<StringReadAccess> getAccess().getStringValue()).isEqualTo("foo");
        assertThat(readList.isMissing(1)).isTrue();
        assertThat(readList.isMissing(2)).isTrue();
        writeList.setWriteIndex(2);
        readList.setIndex(2);
        writeAccess.setStringValue("bar");
        assertThat(readList.isMissing(1)).isTrue();
        assertThat(readList.isMissing(2)).isFalse();
        assertThat(readList.<StringReadAccess> getAccess().getStringValue()).isEqualTo("bar");

        writeList.setMissing();
        assertThat(readList.isMissing()).isTrue();

        var mockListReadAccess = mock(ListReadAccess.class);
        var mockStringReadAccess = mock(StringReadAccess.class);
        when(mockListReadAccess.getAccess()).thenReturn(mockStringReadAccess);
        when(mockListReadAccess.size()).thenReturn(2);
        when(mockStringReadAccess.isMissing()).thenReturn(true, false);
        when(mockStringReadAccess.getStringValue()).thenReturn("baz");
        writeList.setFrom(mockListReadAccess);
        assertThat(readList.isMissing()).isFalse();
        assertThat(readList.size()).isEqualTo(2);
        assertThat(readList.isMissing(0)).isTrue();
        assertThat(readList.isMissing(1)).isFalse();
        readList.setIndex(1);
        assertThat(readList.<StringReadAccess> getAccess().getStringValue()).isEqualTo("baz");

        when(mockListReadAccess.isMissing()).thenReturn(true);
        writeList.setFrom(mockListReadAccess);
        assertThat(readList.isMissing()).isTrue();
    }

    @Test
    public void testBufferedListAccessCreatesSetsBuffersToMissing() throws Exception {
        var spec = new ListDataSpec(DataSpec.stringSpec());
        var buffer = BufferedAccesses.createBufferedAccess(spec);
        var write = (ListWriteAccess)buffer;
        var read = (ListReadAccess)buffer;

        String[] values = {"foo", "bar", "baz"};
        writeToListAccess(write, values);
        assertThat(listToArray(read)).containsExactly(values);

        String[] valuesWithMissing = {"bli", null, "blub"};
        writeToListAccess(write, valuesWithMissing);
        assertThat(listToArray(read)).containsExactly("bli", "?", "blub");
    }

    /**
     * Test whether switching between ByteArray and Object access works in BufferedVarBinaryAcces, no matter whether a
     * ByteArray or an Object was set.
     *
     * @throws IOException
     */
    @Test
    public void testBufferedVarBinaryAccess() throws IOException {
        var buffer = BufferedAccesses.createBufferedAccess(VarBinaryDataSpec.INSTANCE);
        byte[] blob = "TestData".getBytes();
        byte[] blob2 = "FooBar".getBytes();

        ObjectSerializer<byte[]> serializer = (out, v) -> out.write(v);
        ObjectDeserializer<byte[]> deserializer = (in) -> in.readBytes();

        ((VarBinaryWriteAccess)buffer).setByteArray(blob);
        assertArrayEquals(blob, ((VarBinaryReadAccess)buffer).getByteArray());
        assertArrayEquals(blob, ((VarBinaryReadAccess)buffer).getObject(deserializer));

        ((VarBinaryWriteAccess)buffer).setObject(blob2, serializer);
        assertArrayEquals(blob2, ((VarBinaryReadAccess)buffer).getByteArray());
        assertArrayEquals(blob2, ((VarBinaryReadAccess)buffer).getObject(deserializer));

        ((VarBinaryWriteAccess)buffer).setByteArray(blob);
        assertArrayEquals(blob, ((VarBinaryReadAccess)buffer).getByteArray());
        assertArrayEquals(blob, ((VarBinaryReadAccess)buffer).getObject(deserializer));
    }

    private static String[] listToArray(final ListReadAccess readAccess) {
        var values = new String[readAccess.size()];
        StringReadAccess elementAccess = readAccess.getAccess();
        for (int i = 0; i < values.length; i++) {
            readAccess.setIndex(i);
            if (readAccess.isMissing(i)) {
                values[i] = "?";
            } else {
                values[i] = elementAccess.getStringValue();
            }
        }
        return values;
    }

    private static void writeToListAccess(final ListWriteAccess writeAccess, final String... values) {
        StringWriteAccess elementAccess = writeAccess.getWriteAccess();
        writeAccess.create(values.length);
        for (int i = 0; i < values.length; i++) {
            writeAccess.setWriteIndex(i);
            var value = values[i];
            if (value != null) {
                elementAccess.setStringValue(value);
            }
        }
    }
}
