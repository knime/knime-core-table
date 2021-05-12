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
 *   Created on May 13, 2021 by marcel
 */
package org.knime.core.table.virtual;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.schema.ColumnarSchema;

final class IdentityTableTransformSpec implements TableTransformSpec {

    public static final IdentityTableTransformSpec INSTANCE = new IdentityTableTransformSpec();

    private IdentityTableTransformSpec() {}

    @Override
    public List<ColumnarSchema> transformSchemas(final List<ColumnarSchema> schemas) {
        return new ArrayList<>(schemas);
    }

    @Override
    public List<RowAccessible> transformTables(final List<RowAccessible> tables) {
        return new ArrayList<>(tables);
    }

    public static final class IdentityTableTransformSpecSerializer
        extends AbstractTableTransformSpecSerializer<IdentityTableTransformSpec> {

        public IdentityTableTransformSpecSerializer() {
            super(IdentityTableTransformSpec.class, 0);
        }

        @Override
        public void write(final IdentityTableTransformSpec object, final DataOutput output) throws IOException {
            // Nothing to do.
        }

        @Override
        public IdentityTableTransformSpec read(final DataInput input) throws IOException, ClassNotFoundException {
            return INSTANCE;
        }
    }
}
