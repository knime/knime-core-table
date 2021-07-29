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
import java.util.List;

import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.DefaultColumnarSchema;
import org.knime.core.table.schema.traits.DataTraits;

/**
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public final class ColumnarSchemas {

    private ColumnarSchemas() {}

    public static ColumnarSchema append(final List<ColumnarSchema> schemas) {
        int totalNumColumns = 0;
        for (final ColumnarSchema schema : schemas) {
            totalNumColumns += schema.numColumns();
        }
        final List<DataSpec> appendedSpecs = new ArrayList<>(totalNumColumns);
        final List<DataTraits> appendedTraits = new ArrayList<>(totalNumColumns);
        for (final ColumnarSchema schema : schemas) {
            final int numColumns = schema.numColumns();
            for (int i = 0; i < numColumns; i++) {
                appendedSpecs.add(schema.getSpec(i));
                appendedTraits.add(schema.getTraits(i));
            }
        }
        return new DefaultColumnarSchema(appendedSpecs, appendedTraits);
    }

    public static ColumnarSchema concatenate(final List<ColumnarSchema> schemas) {
        final ColumnarSchema schema = schemas.get(0);
        for (int i = 1; i < schemas.size(); i++) {
            // TODO implement read hierarchy for data (e.g. IntData is-a DoubleData)
            final ColumnarSchema furtherSchema = schemas.get(i);
            if (!schema.equals(furtherSchema)) {
                throw new IllegalArgumentException("Incompatible schemas: " + schema + " vs " + furtherSchema);
            }
        }
        return schema;
    }

    public static ColumnarSchema filter(final ColumnarSchema schema, final int[] selection) {
        final DataSpec[] filteredSpecs = new DataSpec[selection.length];
        final DataTraits[] filteredTraits = new DataTraits[selection.length];
        for (int i = 0; i < selection.length; i++) {
            filteredSpecs[i] = schema.getSpec(selection[i]);
            filteredTraits[i] = schema.getTraits(selection[i]);
        }
        return new DefaultColumnarSchema(filteredSpecs, filteredTraits);
    }

    // TODO interface for mapping - beneficial for e.g. wide tables
    public static ColumnarSchema permute(final ColumnarSchema in, final int[] mapping) {
        int[] permutation = mapping;
        if (permutation.length != in.numColumns()) {
            throw new IllegalArgumentException("Number of permutation indices and number of input columns differ: " +
                permutation.length + " vs " + in.numColumns());
        }
        final DataSpec[] permutedSpecs = new DataSpec[permutation.length];
        final DataTraits[] permutedTraits = new DataTraits[permutation.length];
        for (int i = 0; i < permutation.length; i++) {
            permutedSpecs[i] = in.getSpec(permutation[i]);
            permutedTraits[i] = in.getTraits(permutation[i]);
        }
        return new DefaultColumnarSchema(permutedSpecs, permutedTraits);
    }
}
