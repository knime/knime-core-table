package org.knime.core.table.virtual.expression;

import java.util.List;
import java.util.function.Function;
import java.util.function.IntFunction;

import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.DataSpecs.DataSpecWithTraits;
import org.knime.core.table.schema.traits.DefaultDataTraits;
import org.knime.core.table.virtual.VirtualTable;
import org.knime.core.table.virtual.expression.Exec.Computer;
import org.knime.core.table.virtual.expression.ExpressionGrammar.Expr;
import org.rekex.parser.ParseResult;
import org.rekex.parser.ParseResult.Full;
import org.rekex.parser.PegParser;

public class VT {

    /**
     * TODO This should become a non-static method in VirtualTable, with signature
     *      {@code     public VirtualTable map(final String expression, final MapperFactory mapperFactory)    }
     *      similar to the existing
     *      {@code     public VirtualTable map(final int[] columnIndices, final MapperFactory mapperFactory)  }
     *
     *
     * @param table input table
     * @param expression expression that computes a new column
     * @param outputSpec DataSpec of the computed column. Must be assignable from the result type of the expression
     * @return
     */
    public static VirtualTable map(final VirtualTable table, final String expression, final DataSpecWithTraits outputSpec) {
        System.out.println("expression = " + expression);
        final PegParser<Expr> parser = ExpressionGrammar.parser();
        final ParseResult<Expr> result = parser.parse(expression);
        if (result instanceof Full<Expr> full) {
            final Ast.Node ast = full.value().ast();
            final List<Ast.Node> postorder = Ast.postorder(ast);
            final Ast.RequiredColumns columns = Ast.getRequiredColumns(postorder);

            final ColumnarSchema schema = table.getSchema();
            final IntFunction<AstType> columnIndexToAstType = columnIndex -> schema.getSpec(columnIndex).accept(Typing.toAstType);
            Typing.inferTypes(postorder, columnIndexToAstType);

            final IntFunction<Function<ReadAccess[], ? extends Computer>> columnIndexToComputerFactory =
                    columnIndex -> {
                        int inputIndex = columns.getInputIndex(columnIndex);
                        Function<ReadAccess, ? extends Computer> createComputer =
                                schema.getSpec(columnIndex).accept(Exec.toReaderFactory);
                        return readAccesses -> createComputer.apply(readAccesses[inputIndex]);
                    };

            var mapperFactory = Exec.createMapperFactory(ast, columnIndexToComputerFactory, outputSpec);
            return table.map(columns.columnIndices(), mapperFactory);
        } else {
            System.err.println(result);
            throw new IllegalArgumentException();
        }
    }

    public static VirtualTable map(final VirtualTable table, final String expression, final DataSpec outputSpec) {
        return map(table, expression, new DataSpecWithTraits(outputSpec, DefaultDataTraits.EMPTY));
    }

    /**
     * TODO javadoc
     *
     * @param table
     * @param expression
     * @return
     */
    public static VirtualTable filterRows(final VirtualTable table, final String expression) {
        final PegParser<Expr> parser = ExpressionGrammar.parser();
        final ParseResult<Expr> result = parser.parse(expression);
        if (result instanceof Full<Expr> full) {
            final Ast.Node ast = full.value().ast();
            final List<Ast.Node> postorder = Ast.postorder(ast);
            final Ast.RequiredColumns columns = Ast.getRequiredColumns(postorder);

            final ColumnarSchema schema = table.getSchema();
            final IntFunction<AstType> columnIndexToAstType = columnIndex -> schema.getSpec(columnIndex).accept(Typing.toAstType);
            Typing.inferTypes(postorder, columnIndexToAstType);

            final IntFunction<Function<ReadAccess[], ? extends Computer>> columnIndexToComputerFactory =
                    columnIndex -> {
                        int inputIndex = columns.getInputIndex(columnIndex);
                        Function<ReadAccess, ? extends Computer> createComputer =
                                schema.getSpec(columnIndex).accept(Exec.toReaderFactory);
                        return readAccesses -> createComputer.apply(readAccesses[inputIndex]);
                    };

            var filterFactory = Exec.createRowFilterFactory(ast, columnIndexToComputerFactory);
            return table.filterRows(columns.columnIndices(), filterFactory);
        } else {
            System.err.println(result);
            throw new IllegalArgumentException();
        }
    }
}
