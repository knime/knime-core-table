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
 */
package org.knime.core.table.virtual.graph.rag;

import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.Iterator;

/**
 * {@code AccessIds} is a collection of {@link AccessId}.
 * <p>
 * It is used for representing the set of accesses that are required (inputs) or
 * produced (outputs) by a {@link RagNode}. For both inputs and outputs, the
 * accesses are identified by a column index. This matches the {@code VirtualTable}
 * spec.
 * <p>
 * Inputs and outputs may be sparsely populated with respect to column index. For
 * example, if certain columns of a source table are never used in a virtual table
 * construction, the output {@code AccessIds} of the {@code SOURCE} node will have
 * "holes" at the respective column indices.
 * <p>
 * The index of an access in the {@code AccessIds} collection (ordered by column
 * index) is called "slot index". That is, the {@link AccessId} at slot index 0 is
 * the smallest "non-hole" column index.
 */
public class AccessIds extends AbstractCollection<AccessId> {

    private int[] cols;
    private AccessId[] ids;
    private int size;

    public AccessIds() {
        this(8); // TODO: should be always with capacity
    }

    public AccessIds(final int capacity) {
        cols = new int[capacity];
        ids = new AccessId[capacity];
        size = 0;
    }

    public void putAtColumnIndex(final AccessId id, final int columnIndex) {
        final int i = Arrays.binarySearch(cols, 0, size, columnIndex);
        if (i >= 0) {
            cols[i] = columnIndex;
            ids[i] = id;
        } else {
            final int j = -(i + 1); // insertion index
            final int capacity = cols.length;
            if ( size < capacity ) {
                // shift elements [j, size) to [j+1, size+1)
                System.arraycopy(cols, j, cols, j+1, size-j);
                System.arraycopy(ids, j, ids, j+1, size-j);
            } else {
                final int[] oldCols = cols;
                final AccessId[] oldIds = ids;
                cols = new int[2 * capacity];
                ids = new AccessId[2 * capacity];
                // copy elements [0, j) to [0, j)
                System.arraycopy(oldCols, 0, cols, 0, j);
                System.arraycopy(oldIds, 0, ids, 0, j);
                // copy elements [j, size) to [j+1, size+1)
                System.arraycopy(oldCols, j, cols, j+1, size-j);
                System.arraycopy(oldIds, j, ids, j+1, size-j);
            }
            cols[j] = columnIndex;
            ids[j] = id;
            ++size;
        }
    }

    /**
     * Returns the {@code AccessId} at {@code columnIndex}, or {@code null} if no such {@code AccessId} exists.
     */
    public AccessId getAtColumnIndex(final int columnIndex) {
        final int i = Arrays.binarySearch(cols, 0, size, columnIndex);
        return i >= 0 ? ids[i] : null;
    }

    @Override
    public Iterator<AccessId> iterator() {
        return new Iterator<>() {
            int i = 0;

            @Override
            public boolean hasNext() {
                return i < size;
            }

            @Override
            public AccessId next() {
                return i < size ? ids[i++] : null;
            }
        };
    }

    @Override
    public int size() {
        return size;
    }

    public AccessId getAtSlot(final int slotIndex) {
        return ids[slotIndex];
    }

    public int columnAtSlot(final int slotIndex) {
        return cols[slotIndex];
    }

    public int slotIndexOf(final AccessId accessId) {
        for (int i = 0; i < size; i++) {
            if(ids[i].equals(accessId)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Replace all instances of {@code oldId} in this collection with {@code newId}.
     *
     * @param oldId access to be replaced
     * @param newId access to replace it with
     * @return {@code true} iff any replacement was made (that is, iff {@code oldId} occurred in this collection)
     */
    public boolean replace(final AccessId oldId, final AccessId newId) {
        boolean replaced = false;
        for (int i = 0; i < size; i++) {
            if(ids[i].equals(oldId)) {
                ids[i] = newId;
                replaced = true;
            }
        }
        return replaced;
    }
}
