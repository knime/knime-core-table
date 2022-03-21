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

import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.knime.core.table.schema.traits.DataTraits;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

/**
 * Default implementation of an annotated columnar schema
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public class DefaultAnnotatedColumnarSchema implements AnnotatedColumnarSchema {

    private final SchemaMetaData m_schemaMetaData;

    private final ColumnarSchema m_schema;

    private final String[] m_columnNames;

    private final ColumnMetaData[] m_columnMetaData;

    private DefaultAnnotatedColumnarSchema(final ColumnarSchema schema, final String[] columnNames,
        final ColumnMetaData[] columnMetaData, final SchemaMetaData schemaMetaData) {
        if (columnNames.length != schema.numColumns()) {
            throw new IllegalArgumentException(
                "The number of column names must match the number of columns in the schema");
        }

        if (columnMetaData.length != schema.numColumns()) {
            throw new IllegalArgumentException(
                "The number of column meta data fields must match the number of columns in the schema");
        }

        m_schema = schema;
        m_columnNames = columnNames;
        m_columnMetaData = columnMetaData;
        m_schemaMetaData = schemaMetaData;
    }

    /**
     * Annotate a {@link ColumnarSchema} with column names and per-column meta data and schema-wide meta data
     *
     * @param schema The {@link ColumnarSchema}
     * @param columnNames The column names
     * @param columnMetaData The {@link AnnotatedColumnarSchema.ColumnMetaData} per column
     * @param schemaMetaData The {@link AnnotatedColumnarSchema.SchemaMetaData} for the schema
     * @return The {@link AnnotatedColumnarSchema}
     */
    public static AnnotatedColumnarSchema annotate(final ColumnarSchema schema, final String[] columnNames,
        final ColumnMetaData[] columnMetaData, final SchemaMetaData schemaMetaData) {
        return new DefaultAnnotatedColumnarSchema(schema, columnNames, columnMetaData, schemaMetaData);
    }

    /**
     * Annotate a {@link ColumnarSchema} with column names and per-column meta data
     *
     * @param schema The {@link ColumnarSchema}
     * @param columnNames The column names
     * @param columnMetaData The {@link AnnotatedColumnarSchema.ColumnMetaData} per column
     * @return The {@link AnnotatedColumnarSchema}
     */
    public static AnnotatedColumnarSchema annotate(final ColumnarSchema schema, final String[] columnNames,
        final ColumnMetaData[] columnMetaData) {
        return new DefaultAnnotatedColumnarSchema(schema, columnNames, columnMetaData, null);
    }

    /**
     * Annotate a {@link ColumnarSchema} with column names
     *
     * @param schema The {@link ColumnarSchema}
     * @param columnNames The column names
     * @return The {@link AnnotatedColumnarSchema}
     */
    public static AnnotatedColumnarSchema annotate(final ColumnarSchema schema, final String[] columnNames) {
        return new DefaultAnnotatedColumnarSchema(schema, columnNames, new DefaultColumnMetaData[schema.numColumns()],
            null);
    }

    @Override
    public int numColumns() {
        return m_schema.numColumns();
    }

    @Override
    public DataSpec getSpec(final int index) {
        return m_schema.getSpec(index);
    }

    @Override
    public Stream<DataSpec> specStream() {
        return m_schema.specStream();
    }

    @Override
    public DataTraits getTraits(final int index) {
        return m_schema.getTraits(index);
    }

    @Override
    public Iterator<DataSpec> iterator() {
        return m_schema.iterator();
    }

    @Override
    public String getColumnName(final int columnIndex) {
        return m_columnNames[columnIndex];
    }

    @Override
    public ColumnMetaData getColumnMetaData(final int columnIndex) {
        return m_columnMetaData[columnIndex];
    }

    @Override
    public SchemaMetaData getMetaData() {
        return m_schemaMetaData;
    }

    /**
     * Default implementation of {@link AnnotatedColumnarSchema.ColumnMetaData}, which is currently empty and thus a
     * singleton.
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    public static class DefaultColumnMetaData implements ColumnMetaData {
        /** the instance */
        public static final DefaultColumnMetaData INSTANCE = new DefaultColumnMetaData();

        private DefaultColumnMetaData() {
        }

        @Override
        public JsonNode toJson(final JsonNodeFactory factory) {
            return factory.nullNode();
        }
    }

    /**
     * Default implementation of {@link AnnotatedColumnarSchema.SchemaMetaData}, which is currently empty and thus a
     * singleton.
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    public static class DefaultSchemaMetaData implements SchemaMetaData {
        /** the instance */
        public static final DefaultSchemaMetaData INSTANCE = new DefaultSchemaMetaData();

        private DefaultSchemaMetaData() {
        }

        @Override
        public JsonNode toJson(final JsonNodeFactory factory) {
            return factory.nullNode();
        }
    }

    @Override
    public String[] getColumnNames() {
        return m_columnNames;
    }

    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof DefaultAnnotatedColumnarSchema)) {
            return false;
        }
        final var other = (DefaultAnnotatedColumnarSchema)obj;

        return m_schema.equals(other.m_schema) //
            && Arrays.equals(m_columnNames, other.m_columnNames) //
            && ((m_schemaMetaData == null && other.m_schemaMetaData == null)
                || m_schemaMetaData.equals(other.m_schemaMetaData)) //
            && Arrays.equals(m_columnMetaData, other.m_columnMetaData);
    }

    @Override
    public String toString() {
        return "AnnotatedSchema (" + Arrays.stream(m_columnNames).collect(Collectors.toList()) + ") " + m_schema;
    }

    @Override
    public int hashCode() {
        return Objects.hash(m_columnNames, m_columnMetaData, m_schema, m_schemaMetaData);
    }

}
