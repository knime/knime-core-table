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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

/**
 * A columnar schema with additional column names and per-column and per-schema meta data.
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public interface AnnotatedColumnarSchema extends ColumnarSchema {

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
