package org.knime.core.table.virtual.expression;

public sealed interface AstNode {

    enum BinaryOp {
        ADD, // arg1 + arg2
        SUB, // arg1 - arg2
        MUL, // arg1 * arg2
		DIV, // arg1 / arg2
		MOD, // arg1 % arg2
    }

    enum UnaryOp {
		NEG, // -a
	}

    record AstIntConst(int value) implements AstNode {}
    record AstColumnRef(String name) implements AstNode {}
    record AstBinaryOp(AstNode arg1, AstNode arg2, BinaryOp op) implements AstNode {}
    record AstUnaryOp(AstNode arg, UnaryOp op) implements AstNode {}
}
