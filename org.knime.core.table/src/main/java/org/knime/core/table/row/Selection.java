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
package org.knime.core.table.row;

import java.util.Arrays;

import org.knime.core.table.schema.ColumnarSchema;

/**
 * Selects a subset of {@link #columns() columns} and a contiguous range of {@link #rows() rows} from a table.
 * <p>
 * {@code Selection} is immutable and has methods to create derived selections. A selection should be typically
 * constructed by taking the selection of the whole table, and restricting it. For example, a selection comprising
 * columns 1..4 and rows 100..199, would be constructed like this:
 * {@code Selection.all().retainColumns(1, 2, 3, 4).retainRows(100, 200)}.
 * <p>
 * The {@link #columns() column selection} works like a mask. {@code
 * Selection.all()} starts with all columns selected. {@link #retainColumns} restricts this selection by retaining only
 * the selected columns. (The order in which the column indices are provided is ignored.)
 *
 * @author Tobias Pietzsch
 */
public interface Selection {

    /**
     * Get the selected columns.
     *
     * @return the selected columns.
     */
    ColumnSelection columns();

    /**
     * Get the selected contiguous row range.
     *
     * @return the selected row range.
     */
    RowRangeSelection rows();

    /**
     * Returns {@code true} if all columns and all rows are selected, otherwise {@code false}.
     *
     * @return {@code true} if all columns and all rows are selected, otherwise {@code false}.
     */
    default boolean allSelected() {
        return columns().allSelected() && rows().allSelected();
    }

    /**
     * Get the {@code Selection} containing all columns and all rows.
     *
     * @return the {@code Selection} containing all columns and all rows.
     */
    static Selection all() {
        return DefaultSelection.ALL;
    }

    /**
     * Returns a new {@code Selection} that restricts this one by retaining only the
     * given {@code columns}. (The order in which the column indices are provided is
     * ignored.)
     *
     * @param columns column indices that should be retained
     * @return a new {@code Selection}
     */
    Selection retainColumns(int... columns);

    /**
     * Returns a new {@code Selection} that restricts this one by retaining only the
     * given {@code columns}.
     *
     * @param columns columns that should be retained
     * @return a new {@code Selection}
     */
    Selection retainColumns(ColumnSelection columns);

    /**
     * Returns a new {@code Selection} that restricts this one by retaining only the
     * given {@code [from,to)} row range (relative to the start of the row range of
     * this selection).
     * <p>
     * {@code from < 0} indicates an unconstrained range, i.e., the resulting selection
     * will be the same as {@code this}.
     *
     * @param from start of row range (inclusive, relative to the start of the row range of this selection)
     * @param to   end of row range (exclusive, relative to the start of the row range of this selection)
     * @return a new {@code Selection}
     */
    Selection retainRows(long from, long to);

    /**
     * Returns a new {@code Selection} that restricts this one by retaining only the
     * row range of the given {@code RowRangeSelection} (relative to the start of the row
     * range of this selection).
     *
     * @param range the row range to retain (relative to the start of the row range of this selection)
     * @return a new {@code Selection}
     */
    Selection retainRows(RowRangeSelection range);

    /**
     * Returns a new {@code Selection} that restricts this one by retaining only the
     * columns and row range of the given {@code selection}, where row range is relative
     * to the start of the row range of this selection).
     *
     * @param selection the columns and row range to retain
     * @return a new {@code Selection}
     */
    Selection retain(Selection selection);

    /**
     * Selects a subset of columns from a table.
     * <p>
     * {@code ColumnSelection} works like a mask. {@code ColumnSelection.all()} starts with everything selected.
     * {@link #retain(int...)}} restricts this selection by retaining only the given columns. (The order in which the
     * column indices are given is ignored.)
     *
     * @author Tobias Pietzsch
     */
    public interface ColumnSelection {

        /**
         * Get the {@code ColumnSelection} containing all columns.
         *
         * @return the {@code ColumnSelection} containing all columns.
         */
        static ColumnSelection all() {
            return DefaultColumnSelection.ALL;
        }

        /**
         * Returns {@code true} if all columns are selected, otherwise {@code false}.
         *
         * @return {@code true} if all columns are selected, otherwise {@code false}.
         */
        boolean allSelected();

        /**
         * Returns {@code true} if all columns in the specified column range are selected,
         * otherwise {@code false}.
         *
         * @param fromIndex start of columns range (inclusive)
         * @param toIndex end of column range (exclusive)
         * @return {@code true} if all columns in the specified range are selected, otherwise {@code false}.
         * @throws IllegalArgumentException
         *         if {@code fromIndex > toIndex}
         */
        boolean allSelected(int fromIndex, int toIndex);

        /**
         * Returns {@code true} if all columns in the specified schema, otherwise {@code false}.
         * This is a just short-cut for {@code allSelected(0, schema.numColumns())}.
         */
        default boolean allSelected(final ColumnarSchema schema) {
            return allSelected(0, schema.numColumns());
        }

        /**
         * Get the indices of selected columns in a sorted array.
         * If {@link #allSelected()}{@code ==true}, the result is undefined.
         * (In particular, the result may be {@code null}).
         *
         * @return sorted array of column indices (if {@link #allSelected()}{@code ==false}).
         */
        int[] getSelected();

        /**
         * Get the indices of selected columns in the specified column range,
         * as a sorted array.
         *
         * @param fromIndex start of columns range (inclusive)
         * @param toIndex end of column range (exclusive)
         * @return sorted array of column indices
         * @throws IllegalArgumentException
         *         if {@code fromIndex > toIndex}
         */
        int[] getSelected(int fromIndex, int toIndex);

        /**
         * Determines whether a column at a given index is selected.
         *
         * @param index the index of the column
         * @return true if the column is selected, otherwise false
         */
        default boolean isSelected(final int index) {
            return allSelected() || Arrays.binarySearch(getSelected(), index) >= 0;
        }

        /**
         * Restrict this {@code ColumnSelection} by retaining only the given {@code columns}.
         * (The order in which the column indices are provided is ignored.)
         *
         * @param columns column indices that should be retained
         * @return a new {@link ColumnSelection}
         */
        ColumnSelection retain(int... columns);

        /**
         * Restrict this {@code ColumnSelection} by retaining only the given {@code columns}.
         *
         * @param columns columns that should be retained
         * @return a new {@link ColumnSelection}
         */
        default ColumnSelection retain(final ColumnSelection columns) {
            return columns.allSelected() ? this : retain(columns.getSelected());
        }
    }

    /**
     * Defines a row range selection by the start (inclusive) and end (exclusive) rows.
     * The {@link #allSelected() full range of all rows} is indicated by {@link #fromIndex()}<0.
     *
     * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
     */
    public interface RowRangeSelection {

        /**
         * Get the {@code RowRangeSelection} containing all rows.
         *
         * @return the {@code RowRangeSelection} containing all rows.
         */
        static RowRangeSelection all() {
            return DefaultRowRangeSelection.ALL;
        }

        /**
         * Returns {@code true} if all rows are selected, otherwise {@code false}.
         *
         * @return {@code true} if all rows are selected, otherwise {@code false}.
         */
        boolean allSelected();

        /**
         * Returns {@code true} if all rows in the given range are selected, otherwise {@code false}.
         *
         * @param from start of row range (inclusive)
         * @param to   end of row range (exclusive)
         * @return {@code true} if all rows in the given range are selected, otherwise {@code false}.
         */
        boolean allSelected(long from, long to);

        /**
         * Get the start of row range (inclusive).
         * If {@link #allSelected()}{@code ==true}, then {@code fromIndex() < 0}.
         *
         * @return start of row range (inclusive), or a negative value if the range contains all rows
         */
        long fromIndex();

        /**
         * Get the end of row range (exclusive).
         * If {@link #allSelected()}{@code ==true}, the result is undefined.
         *
         * @return end of row range (exclusive)
         */
        long toIndex();

        /**
         * Restrict this {@link RowRangeSelection} by retaining only the given {@code [from,to)}
         * range (relative to the start of this selection).
         * <p>
         * {@code from < 0} indicates an unconstrained range, i.e., the resulting selection
         * will be the same as {@code this}.
         * <p>
         * {@code to < from} is permitted and indicates an empty row range (if {@code from >= 0}).
         *
         * @param from start of row range (inclusive, relative to the start of this RowRangeSelection)
         * @param to   end of row range (exclusive, relative to the start of this RowRangeSelection)
         * @return a new {@link RowRangeSelection}
         */
        RowRangeSelection retain(long from, long to);

        /**
         * Restrict this {@link RowRangeSelection} by retaining only the given range (relative
         * to the start of this selection).
         *
         * @param rows the row range to retain (relative to the start of this RowRangeSelection)
         * @return a new {@link RowRangeSelection}
         */
        default RowRangeSelection retain(final RowRangeSelection rows) {
            return rows.allSelected() ? this : retain(rows.fromIndex(), rows.toIndex());
        }
    }

}
