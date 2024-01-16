package org.knime.core.table.virtual.expression;

public enum AstType {
    BYTE(true), //
    INT(true), //
    LONG(true), //
    FLOAT(true), //
    DOUBLE(true), //
    BOOLEAN(false), //
    STRING(false);

    private final boolean isNumeric;

    AstType(final boolean isNumeric) {
        this.isNumeric = isNumeric;
    }

    boolean isNumeric() {
        return isNumeric;
    }
}
