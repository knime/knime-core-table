package org.knime.core.table.row;

class DefaultSelection implements Selection {

    private final ColumnSelection m_columns;

    private final RowRangeSelection m_rows;

    static final Selection ALL = new DefaultSelection(new DefaultColumnSelection(), new DefaultRowRangeSelection());

    public DefaultSelection(final ColumnSelection columns, final RowRangeSelection rows) {
        this.m_columns = columns;
        this.m_rows = rows;
    }

    @Override
    public ColumnSelection columns() {
        return m_columns;
    }

    @Override
    public RowRangeSelection rows() {
        return m_rows;
    }

    @Override
    public Selection retainColumns(final int... columns) {
        return new DefaultSelection(columns().retain(columns), m_rows);
    }

    @Override
    public Selection retainColumns(final ColumnSelection selection) {
        return new DefaultSelection(m_columns.retain(selection), m_rows);
    }

    @Override
    public Selection retainRows(final long from, final long to) {
        return new DefaultSelection(m_columns, m_rows.retain(from, to));
    }

    @Override
    public Selection retainRows(final RowRangeSelection selection) {
        return new DefaultSelection(m_columns, m_rows.retain(selection));
    }

    @Override
    public Selection retain(final Selection selection) {
        return new DefaultSelection(m_columns.retain(selection.columns()), m_rows.retain(selection.rows()));
    }

    @Override
    public int hashCode() {
        return m_columns.hashCode() * 31 + m_rows.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Selection)) {
            return false;
        }
        Selection that = (Selection)obj;
        return m_columns.equals(that.columns()) && m_rows.equals(that.rows());
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("DefaultSelection{");
        sb.append(m_columns);
        sb.append(", ").append(m_rows);
        sb.append('}');
        return sb.toString();
    }
}
