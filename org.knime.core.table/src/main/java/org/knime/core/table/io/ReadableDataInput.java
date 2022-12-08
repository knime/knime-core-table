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
 *   Created on Nov 25, 2021 by Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
package org.knime.core.table.io;

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;

/**
 * A {@link DataInput} that also defines a read method that allows to read up to a certain number of bytes into a byte
 * array. This is akin to {@link InputStream#read(byte[], int, int)}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public interface ReadableDataInput extends DataInput {

    /**
     * This method corresponds to {@link InputStream#read(byte[], int, int)}.<br>
     *
     * Reads up to <code>len</code> bytes of data from the input stream into an array of bytes. An attempt is made to
     * read as many as <code>len</code> bytes, but a smaller number may be read. The number of bytes actually read is
     * returned as an integer.
     *
     *
     * @param b the buffer into which the data is read.
     * @param off the start offset in array <code>b</code> at which the data is written.
     * @param len the maximum number of bytes to read.
     * @return the total number of bytes read into the buffer, or <code>-1</code> if there is no more data because the
     *         end of the stream has been reached.
     * @throws IOException if any other IO problems are encountered
     * @see InputStream#read(byte[], int, int)
     */
    int read(byte[] b, int off, int len) throws IOException;

    /**
     * Read all available bytes until the end of the stream. This is similar to {@link #readFully(byte[])}, however no
     * preallocated buffer must be passed to the function. The amount of data to be read is determined by reading all
     * remaining data until EOF is reached.
     *
     * @return The bytes read from the input stream until
     *
     * @throws IOException if any other IO problems are encountered
     * @throws IndexOutOfBoundsException if the input data contains more than MAX_INT bytes to read, because no byte[]
     *             can be created to contain all the data.
     */
    byte[] readBytes() throws IOException;
}
