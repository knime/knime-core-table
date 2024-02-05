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
public final class DefaultAnnotatedColumnarSchema implements AnnotatedColumnarSchema {

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
    public static final class DefaultColumnMetaData implements ColumnMetaData {
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
    public static final class DefaultSchemaMetaData implements SchemaMetaData {
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
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != this.getClass()) {
            return false;
        }
        final var other = (DefaultAnnotatedColumnarSchema)obj;

        return m_schema.equals(other.m_schema) //
            && Arrays.equals(m_columnNames, other.m_columnNames) //
            && Objects.equals(m_schemaMetaData, other.m_schemaMetaData) //
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
