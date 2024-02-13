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
 *   Created on Apr 23, 2021 by Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
package org.knime.core.table.cursor;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;

/**
 * A {@link Cursor} for writing data to a storage.
 * <p>
 * Provides a {@link #flush()} method that ensures that any data that hasn't been written out, yet, is written out.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 * @author Tobias Pietzsch
 *
 * @param <A> the type of access forwarded by this forwarder
 */
public interface WriteCursor<A> extends Closeable, Flushable {

    /**
     * Flush data that hasn't been written out yet. Does not close the {@link WriteCursor}.
     *
     * @throws IOException if flushing fails
     */
    @Override
    void flush() throws IOException;

    /**
     * Finish writing data that hasn't been written out yet, and close the {@link WriteCursor}.
     *
     * TODO: Clarify javadoc:
     *   What is the difference between "writing data" in flush() and finish()?
     *   In practice, for the arrow implementation:
     *     * finish() will serialize pending data, then close the current batch and close() the WriteCursor.
     *     * flush() will serialize pending data, but will not close the current batch. Therefore, it is not
     *       safe to close() the WriteCursor after flush() without losing data.
     *
     * @throws IOException if writing fails
     */
    void finish() throws IOException;

    // TODO: copied from Cursor
    /**
     * Always returns the same access instance.<br>
     * This method can be called before the first call to {@link #forward()} e.g. to set up a decorator.<br>
     * However, the access won't point to any values and it is only save to access values after the first
     * {@link #forward()} call.
     *
     * @return the access that is forwarded by this cursor
     */
    A access();

    // TODO: copied from Cursor
    /**
     * @return true if forwarding was successful, false otherwise (i.e. at the end)
     */
    boolean forward();
}
