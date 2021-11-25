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

import java.io.DataInputStream;
import java.io.InputStream;

/**
 * A {@link ReadableDataInput} that is based on an input stream.<br>
 * The {@link ReadableDataInput#read(byte[], int, int)} method is equivalent to
 * {@link InputStream#read(byte[], int, int)}, hence no special implementation is needed.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class ReadableDataInputStream extends DataInputStream implements ReadableDataInput {

    /**
     * Creates a ReadableDataInputStream that uses the specified underlying InputStream.
     *
     * @param input underlying InputStream
     */
    public ReadableDataInputStream(final InputStream input) {
        super(input);
    }

}