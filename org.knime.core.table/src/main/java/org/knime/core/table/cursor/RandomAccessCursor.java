package org.knime.core.table.cursor;

/**
 * Cursor that provides random access via the {@link #moveTo(long)} method.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public interface RandomAccessCursor<A> extends LookaheadCursor<A> {

    /**
     * Move this cursor to the specified {@code row}.
     * <p>
     * The effect of {@code moveTo(i)} is that this cursor's {@link #access} refers the same data as if calling
     * {@link #forward} {@code i+1} times on a new cursor.
     *
     * @param row index of the row to move to
     * @throws IndexOutOfBoundsException if {@code row<0} or the table contains less than {@code row-1} rows.
     */
    void moveTo(long row);
}
