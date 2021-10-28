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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.knime.core.table.access.ListAccess.ListReadAccess;
import org.knime.core.table.access.ListAccess.ListWriteAccess;
import org.knime.core.table.access.StringAccess.StringReadAccess;
import org.knime.core.table.access.StringAccess.StringWriteAccess;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.ListDataSpec;
import org.mockito.ArgumentMatchers;

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
        StringWriteAccess firstWriteAccess = writeList.getWriteAccess(0);
        firstWriteAccess.setStringValue("foo");
        assertThat(readList.isMissing(0)).isFalse();
        assertThat(readList.<StringReadAccess>getAccess(0).getStringValue()).isEqualTo("foo");
        assertThat(readList.isMissing(1)).isTrue();
        assertThat(readList.isMissing(2)).isTrue();
        writeList.<StringWriteAccess>getWriteAccess(2).setStringValue("bar");
        assertThat(readList.isMissing(1)).isTrue();
        assertThat(readList.isMissing(2)).isFalse();
        assertThat(readList.<StringReadAccess>getAccess(2).getStringValue()).isEqualTo("bar");

        writeList.setMissing();
        assertThat(readList.isMissing()).isTrue();

        var mockListReadAccess = mock(ListReadAccess.class);
        var mockStringReadAccess = mock(StringReadAccess.class);
        when(mockListReadAccess.getAccess(ArgumentMatchers.anyInt())).thenReturn(mockStringReadAccess);
        when(mockListReadAccess.size()).thenReturn(2);
        when(mockStringReadAccess.isMissing()).thenReturn(true, false);
        when(mockStringReadAccess.getStringValue()).thenReturn("baz");
        writeList.setFrom(mockListReadAccess);
        assertThat(readList.isMissing()).isFalse();
        assertThat(readList.size()).isEqualTo(2);
        assertThat(readList.isMissing(0)).isTrue();
        assertThat(readList.isMissing(1)).isFalse();
        assertThat(readList.<StringReadAccess>getAccess(1).getStringValue()).isEqualTo("baz");

        when(mockListReadAccess.isMissing()).thenReturn(true);
        writeList.setFrom(mockListReadAccess);
        assertThat(readList.isMissing()).isTrue();
    }
}
