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
 *   May 24, 2024 (benjamin): created
 */
package org.knime.core.expressions.aggregations;

import static org.knime.core.expressions.AstTestUtils.BOOL;
import static org.knime.core.expressions.AstTestUtils.INT;
import static org.knime.core.expressions.AstTestUtils.STR;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.knime.core.expressions.Ast.AggregationCall;
import org.knime.core.expressions.ValueType;

/**
 * Tests for the built-in aggregations.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("static-method")
final class BuiltInAggregationsTests {

    private static final String INT_COL = "intCol";

    private static final String FLOAT_COL = "floatCol";

    private static final String STR_COL = "stringCol";

    private static final Map<String, ValueType> COLUMN_TYPES = Map.of( //
        INT_COL, ValueType.OPT_INTEGER, //
        FLOAT_COL, ValueType.OPT_FLOAT, //
        STR_COL, ValueType.OPT_STRING //
    );

    @ParameterizedTest
    @ArgumentsSource(TestColumnAggregationArgumentSource.class)
    void testArgsForAllAggregations(final AggregationCall agg) {
        Assertions.assertNotNull(agg, "No test arguments for " + agg);
    }

    @TestFactory
    List<DynamicNode> max() {
        return new ColumnAggregationTestBuilder(BuiltInAggregations.MAX, COLUMN_TYPES) //
            .typing("Integer column positional", List.of(STR(INT_COL)), Map.of(), ValueType.OPT_INTEGER) //
            .typing("Integer column named", List.of(), Map.of("column", STR(INT_COL)), ValueType.OPT_INTEGER) //
            .typing("Float column positional", List.of(STR(FLOAT_COL)), Map.of(), ValueType.OPT_FLOAT) //
            .typing("Float column named", List.of(), Map.of("column", STR(FLOAT_COL)), ValueType.OPT_FLOAT) //
            .typing("Specify second arg positional", List.of(STR(INT_COL), BOOL(false)), Map.of(), ValueType.OPT_INTEGER) //
            .typing("Specify second arg named", List.of(), Map.of("column", STR(INT_COL), "ignore_nan", BOOL(false)), ValueType.OPT_INTEGER) //
            .illegalArgs("No column arg", List.of(), Map.of()) //
            .illegalArgs("String column", List.of(STR(STR_COL)), Map.of()) //
            .illegalArgs("No column arg", List.of(STR("foo")), Map.of()) //
            .illegalArgs("Invalid second arg type", List.of(STR(INT_COL), STR("foo")), Map.of()) //
            .tests();
    }

    @TestFactory
    List<DynamicNode> min() {
        return new ColumnAggregationTestBuilder(BuiltInAggregations.MIN, COLUMN_TYPES) //
            .typing("Integer column positional", List.of(STR(INT_COL)), Map.of(), ValueType.OPT_INTEGER) //
            .typing("Integer column named", List.of(), Map.of("column", STR(INT_COL)), ValueType.OPT_INTEGER) //
            .typing("Float column positional", List.of(STR(FLOAT_COL)), Map.of(), ValueType.OPT_FLOAT) //
            .typing("Float column named", List.of(), Map.of("column", STR(FLOAT_COL)), ValueType.OPT_FLOAT) //
            .typing("Specify second arg positional", List.of(STR(INT_COL), BOOL(false)), Map.of(), ValueType.OPT_INTEGER) //
            .typing("Specify second arg named", List.of(), Map.of("column", STR(INT_COL), "ignore_nan", BOOL(false)), ValueType.OPT_INTEGER) //
            .illegalArgs("No column arg", List.of(), Map.of()) //
            .illegalArgs("String column", List.of(STR(STR_COL)), Map.of()) //
            .illegalArgs("No column arg", List.of(STR("foo")), Map.of()) //
            .illegalArgs("Invalid second arg type", List.of(STR(INT_COL), STR("foo")), Map.of()) //
            .tests();
    }

    @TestFactory
    List<DynamicNode> sum() {
        return new ColumnAggregationTestBuilder(BuiltInAggregations.SUM, COLUMN_TYPES) //
            .typing("Integer column positional", List.of(STR(INT_COL)), Map.of(), ValueType.OPT_INTEGER) //
            .typing("Integer column named", List.of(), Map.of("column", STR(INT_COL)), ValueType.OPT_INTEGER) //
            .typing("Float column positional", List.of(STR(FLOAT_COL)), Map.of(), ValueType.OPT_FLOAT) //
            .typing("Float column named", List.of(), Map.of("column", STR(FLOAT_COL)), ValueType.OPT_FLOAT) //
            .typing("Specify second arg positional", List.of(STR(INT_COL), BOOL(false)), Map.of(), ValueType.OPT_INTEGER) //
            .typing("Specify second arg named", List.of(), Map.of("column", STR(INT_COL), "ignore_nan", BOOL(false)), ValueType.OPT_INTEGER) //
            .illegalArgs("No column arg", List.of(), Map.of()) //
            .illegalArgs("String column", List.of(STR(STR_COL)), Map.of()) //
            .illegalArgs("No column arg", List.of(STR("foo")), Map.of()) //
            .illegalArgs("Invalid second arg type", List.of(STR(INT_COL), STR("foo")), Map.of()) //
            .tests();
    }

    @TestFactory
    List<DynamicNode> average() {
        return new ColumnAggregationTestBuilder(BuiltInAggregations.AVERAGE, COLUMN_TYPES) //
            .typing("Integer column positional", List.of(STR(INT_COL)), Map.of(), ValueType.OPT_FLOAT) //
            .typing("Integer column named", List.of(), Map.of("column", STR(INT_COL)), ValueType.OPT_FLOAT) //
            .typing("Float column positional", List.of(STR(FLOAT_COL)), Map.of(), ValueType.OPT_FLOAT) //
            .typing("Float column named", List.of(), Map.of("column", STR(FLOAT_COL)), ValueType.OPT_FLOAT) //
            .typing("Specify second arg positional", List.of(STR(INT_COL), BOOL(false)), Map.of(), ValueType.OPT_FLOAT) //
            .typing("Specify second arg named", List.of(), Map.of("column", STR(INT_COL), "ignore_nan", BOOL(false)), ValueType.OPT_FLOAT) //
            .illegalArgs("No column arg", List.of(), Map.of()) //
            .illegalArgs("String column", List.of(STR(STR_COL)), Map.of()) //
            .illegalArgs("No column arg", List.of(STR("foo")), Map.of()) //
            .illegalArgs("Invalid second arg type", List.of(STR(INT_COL), STR("foo")), Map.of()) //
            .tests();
    }

    @TestFactory
    List<DynamicNode> median() {
        return new ColumnAggregationTestBuilder(BuiltInAggregations.MEDIAN, COLUMN_TYPES) //
            .typing("Integer column positional", List.of(STR(INT_COL)), Map.of(), ValueType.OPT_FLOAT) //
            .typing("Integer column named", List.of(), Map.of("column", STR(INT_COL)), ValueType.OPT_FLOAT) //
            .typing("Float column positional", List.of(STR(FLOAT_COL)), Map.of(), ValueType.OPT_FLOAT) //
            .typing("Float column named", List.of(), Map.of("column", STR(FLOAT_COL)), ValueType.OPT_FLOAT) //
            .typing("Specify second arg positional", List.of(STR(INT_COL), BOOL(false)), Map.of(), ValueType.OPT_FLOAT) //
            .typing("Specify second arg named", List.of(), Map.of("column", STR(INT_COL), "ignore_nan", BOOL(false)), ValueType.OPT_FLOAT) //
            .illegalArgs("No column arg", List.of(), Map.of()) //
            .illegalArgs("String column", List.of(STR(STR_COL)), Map.of()) //
            .illegalArgs("No column arg", List.of(STR("foo")), Map.of()) //
            .illegalArgs("Invalid second arg type", List.of(STR(INT_COL), STR("foo")), Map.of()) //
            .tests();
    }

    @TestFactory
    List<DynamicNode> variance() {
        return new ColumnAggregationTestBuilder(BuiltInAggregations.VARIANCE, COLUMN_TYPES) //
            .typing("Integer column positional", List.of(STR(INT_COL)), Map.of(), ValueType.OPT_FLOAT) //
            .typing("Integer column named", List.of(), Map.of("column", STR(INT_COL)), ValueType.OPT_FLOAT) //
            .typing("Float column positional", List.of(STR(FLOAT_COL)), Map.of(), ValueType.OPT_FLOAT) //
            .typing("Float column named", List.of(), Map.of("column", STR(FLOAT_COL)), ValueType.OPT_FLOAT) //
            .typing("Specify second arg positional", List.of(STR(INT_COL), BOOL(false)), Map.of(), ValueType.OPT_FLOAT) //
            .typing("Specify second arg named", List.of(), Map.of("column", STR(INT_COL), "ignore_nan", BOOL(false)), ValueType.OPT_FLOAT) //
            .illegalArgs("No column arg", List.of(), Map.of()) //
            .illegalArgs("String column", List.of(STR(STR_COL)), Map.of()) //
            .illegalArgs("No column arg", List.of(STR("foo")), Map.of()) //
            .illegalArgs("Invalid second arg type", List.of(STR(INT_COL), STR("foo")), Map.of()) //
            .tests();
    }

    @TestFactory
    List<DynamicNode> stddev() {
        return new ColumnAggregationTestBuilder(BuiltInAggregations.STD_DEV, COLUMN_TYPES) //
            .typing("Integer column positional", List.of(STR(INT_COL),BOOL(false)), Map.of(), ValueType.OPT_FLOAT) //
            .typing("Integer column named", List.of(), Map.of("column", STR(INT_COL),"ignore_nan",BOOL(false)), ValueType.OPT_FLOAT) //
            .typing("Float column positional", List.of(STR(FLOAT_COL),BOOL(false)), Map.of(), ValueType.OPT_FLOAT) //
            .typing("Float column named", List.of(), Map.of("column", STR(FLOAT_COL),"ignore_nan",BOOL(false)), ValueType.OPT_FLOAT) //
            .typing("Specify second arg positional", List.of(STR(INT_COL), BOOL(false)), Map.of(), ValueType.OPT_FLOAT) //
            .typing("Specify second arg named", List.of(), Map.of("column", STR(INT_COL), "ignore_nan", BOOL(false)), ValueType.OPT_FLOAT) //
            .typing("All 3 args specified named", List.of(), Map.of("column", STR(INT_COL), "ignore_nan", BOOL(false), "ddof", INT(1)), ValueType.OPT_FLOAT) //
            .typing("All 3 args specified positionally", List.of(STR(INT_COL), BOOL(false), INT(1)), Map.of(), ValueType.OPT_FLOAT) //
            .illegalArgs("No column arg", List.of(), Map.of()) //
            .illegalArgs("String column", List.of(STR(STR_COL)), Map.of()) //
            .illegalArgs("No column arg", List.of(STR("foo")), Map.of()) //
            .illegalArgs("Invalid second arg type", List.of(STR(INT_COL), STR("foo")), Map.of()) //
            .illegalArgs("Invalid third arg type", List.of(STR(INT_COL), BOOL(false), STR("foo")), Map.of()) //
            .tests();
    }

    @TestFactory
    List<DynamicNode> count() {
        return new ColumnAggregationTestBuilder(BuiltInAggregations.COUNT, COLUMN_TYPES) //
            .typing("Integer column positional", List.of(STR(INT_COL), BOOL(true)), Map.of(), ValueType.INTEGER) //
            .typing("Integer column named", List.of(), Map.of("column", STR(INT_COL), "ignore_missing", BOOL(true)), ValueType.INTEGER) //
            .typing("Both positional", List.of(STR(STR_COL), BOOL(true)), Map.of(), ValueType.INTEGER) //
            .typing("Both named", List.of(), Map.of("column", STR(STR_COL), "ignore_missing", BOOL(true)), ValueType.INTEGER) //
            .typing("String column", List.of(STR(STR_COL)), Map.of(), ValueType.INTEGER) //
            .illegalArgs("No column arg", List.of(), Map.of()) //
            .illegalArgs("Invalid column", List.of(STR("foo")), Map.of()) //
            .illegalArgs("Invalid second arg type", List.of(STR(INT_COL), STR("foo")), Map.of()) //
            .tests();
    }
}
