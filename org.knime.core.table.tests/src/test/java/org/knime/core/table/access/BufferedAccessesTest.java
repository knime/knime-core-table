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
