package org.knime.core.table.row;

import org.knime.core.table.row.Selection.RowRangeSelection;

class DefaultRowRangeSelection implements RowRangeSelection {

    private final long m_from;

    private final long m_to;

    static final RowRangeSelection ALL = new DefaultRowRangeSelection();

    public DefaultRowRangeSelection() {
        this(-1, -1);
    }

    public DefaultRowRangeSelection(final long from, final long to) {
        this.m_from = from;
        this.m_to = Math.max(from, to); // if (to < from), the row range is empty
    }

    @Override
    public boolean allSelected() {
        return m_from < 0;
    }

    @Override
    public long fromIndex() {
        return m_from;
    }

    @Override
    public long toIndex() {
        return m_to;
    }

    @Override
    public RowRangeSelection retain(final long from, final long to) {
        if (allSelected()) {
            return new DefaultRowRangeSelection(from, to);
        } else if (from < 0) {
            return this;
        } else {
            return new DefaultRowRangeSelection(//
                    Math.min(this.m_to, this.m_from + from),//
                    Math.min(this.m_to, this.m_from + to));
        }
    }

    @Override
    public int hashCode() {
        return Long.hashCode(m_from) * 31 + Long.hashCode(m_to);
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof RowRangeSelection)) { // NOSONAR
            return false;
        }
        final RowRangeSelection that = (RowRangeSelection)obj;
        if (allSelected() != that.allSelected()) {
            return false;
        }
        if (allSelected() && that.allSelected()) {
            return true;
        }
        return m_from == that.fromIndex() && m_to == that.toIndex();
    }

    @Override
    public String toString() {
        if (allSelected()) {
            return "select all rows";
        } else {
            final StringBuilder sb = new StringBuilder("select rows");
            sb.append(" from=").append(m_from);
            sb.append(" to=").append(m_to);
            return sb.toString();
        }
    }
}
