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
 *   Oct 9, 2020 (benjamin): created
 */
package org.knime.core.table.access;

import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.ListDataSpec;

/**
 * Definitions of Access for to Lists.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 * @since 4.3
 *
 * @noreference This class is not intended to be referenced by clients.
 */
public final class ListAccess {

    private ListAccess() {
    }

    /**
     * {@link ReadAccess} to a list.
     *
     * @since 4.3
     */
    public interface ListReadAccess extends ReadAccess {

        /**
         * Get the {@link ReadAccess} at the current index in the list. Note that this object should only be used until
         * this method is called again. Implementations are allowed to reuse the object when this method is called
         * again.
         *
         * @param <R> the type of the {@link ReadAccess}
         * @return the {@link ReadAccess} at the given index
         */
        <R extends ReadAccess> R getAccess();

        /**
         * Sets the index at which the access provided by {@link #getAccess()} reads.
         *
         * @param index to read from
         */
        void setIndex(int index);

        /**
         * @param index the index in the list
         * @return <code>true</code> if the value at this index is missing
         */
        boolean isMissing(int index);

        /**
         * @return the size of the list
         */
        int size();

        @Override
        default DataSpec getDataSpec() {
            if (size() < 1) {
                throw new IllegalStateException("Cannot get the DataSpec of a list without inner accesses");
            }
            final var innerSpec = getAccess().getDataSpec();
            return new ListDataSpec(innerSpec);
        }
    }

    /**
     * {@link WriteAccess} to a list.
     *
     * @since 4.3
     */
    public interface ListWriteAccess extends WriteAccess {

        /**
         * Get the {@link WriteAccess} at the given index in the list. Call this only after starting a new list with
         * {@link #create(int)}.
         *
         * Note that this object should only be used until this method is called again. Implementations are allowed to
         * reuse the object when this method is called again.
         *
         * @param <W> the type of the {@link WriteAccess}
         * @param index the index in the list
         * @return the {@link WriteAccess} at the given index
         */
        <W extends WriteAccess> W getWriteAccess();

        /**
         * Sets the index at which the access provided via {@link #getWriteAccess()} writes.
         *
         * @param index to write at
         */
        void setWriteIndex(final int index);

        /**
         * Create a new list with the given size. Call this before accessing the elements.
         *
         * @param size the size of the list
         */
        void create(int size);
    }
}
