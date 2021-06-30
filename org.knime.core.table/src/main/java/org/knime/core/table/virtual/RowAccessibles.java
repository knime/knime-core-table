/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any questions please contact the copyright holder:
 * website: www.knime.com
 * email: contact@knime.com
 * ---------------------------------------------------------------------
 *
 * History
 *   Created on May 21, 2021 by dietzc
 */
package org.knime.core.table.virtual;

import java.util.ArrayList;
import java.util.Arrays;

import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public final class RowAccessibles {

    private RowAccessibles() {}

    public static RowAccessible append(final RowAccessible in, final RowAccessible... other) {
        final ArrayList<RowAccessible> list = new ArrayList<>();
        if (in instanceof AppendedRowAccessible) {
            list.addAll(((AppendedRowAccessible)in).getAppendedTables());
        } else {
            list.add(in);
        }
        list.addAll(Arrays.asList(other));
        return new AppendedRowAccessible(list);
    }

    public static RowAccessible appendMissing(final RowAccessible in, final ColumnarSchema appened) {
        return new AppendedMissingRowAccessible(in, appened);
    }

    public static RowAccessible concatenate(final RowAccessible in, final RowAccessible... other) {
        final ArrayList<RowAccessible> list = new ArrayList<>();
        if (in instanceof ConcatenatedRowAccessible) {
            list.addAll(((ConcatenatedRowAccessible)in).getConcatenatedRowAccessibles());
        } else {
            list.add(in);
        }
        list.addAll(Arrays.asList(other));
        return new ConcatenatedRowAccessible(list);
    }

    public static RowAccessible filter(final RowAccessible in, final int[] selection) {
        return new ColumnFilteredRowAccessible(in, selection);
    }

    public static RowAccessible permute(final RowAccessible in, final int[] mapping) {
        return new PermutedRowAccessible(in, mapping);
    }

    public static RowAccessible slice(final RowAccessible in, final RowRangeSelection selection) {
        return new SlicedRowAccessible(in, selection.fromIndex(), selection.toIndex());
    }
}
