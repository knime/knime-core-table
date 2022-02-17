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
 *   Created on 16 Feb 2022 by chaubold
 */
package org.knime.core.table.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

/**
 * A columnar schema with additional column names and per-column and per-schema meta data.
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public interface AnnotatedColumnarSchema extends ColumnarSchema {

    /**
     * @return The underlying {@link ColumnarSchema}
     */
    ColumnarSchema getColumnarSchema();

    /**
     * Get the name of the column at the specified index
     *
     * @param columnIndex The index of the column
     * @return The column name
     */
    String getColumnName(final int columnIndex);

    /**
     * Get the {@link ColumnMetaData} of the specified column
     *
     * @param columnIndex The index of the column
     * @return The {@link ColumnMetaData} of the column. Can be null if no meta data was set.
     */
    ColumnMetaData getColumnMetaData(final int columnIndex);

    /**
     * @return The {@link SchemaMetaData} attached to the schema, can be null if no meta data was set.
     */
    SchemaMetaData getMetaData();

    /**
     * Interface for meta data attached to a column. We expect this meta data to be serializable to JSON.
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    public interface ColumnMetaData {
        /**
         * Return a JSON representation of the {@link ColumnMetaData}
         * @param factory The {@link JsonNodeFactory} to use for serialization
         * @return A JsonNode representing the {@link ColumnMetaData}
         */
        JsonNode toJson(JsonNodeFactory factory);
    }

    /**
     * Interface for meta data attached to the schema. We expect this meta data to be serializable to JSON.
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    public interface SchemaMetaData {
        /**
         * Return a JSON representation of the {@link SchemaMetaData}
         * @param factory The {@link JsonNodeFactory} to use for serialization
         * @return A JsonNode representing the {@link SchemaMetaData}.
         */
        JsonNode toJson(JsonNodeFactory factory);
    }

    /**
     * @return All column names
     */
    String[] getColumnNames();
}
