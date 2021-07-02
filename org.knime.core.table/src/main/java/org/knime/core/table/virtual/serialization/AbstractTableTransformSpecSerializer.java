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
 *   Apr 23, 2021 (marcel): created
 */
package org.knime.core.table.virtual.serialization;

import org.knime.core.table.virtual.spec.TableTransformSpec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public abstract class AbstractTableTransformSpecSerializer<T extends TableTransformSpec>
    implements TableTransformSpecSerializer<T> {

    private final String m_transformIdentifier;

    private final int m_version;

    public AbstractTableTransformSpecSerializer(final String transformIdentifier, final int serializerVersion) {
        m_transformIdentifier = transformIdentifier;
        m_version = serializerVersion;
    }

    @Override
    public String getTransformIdentifier() {
        return m_transformIdentifier;
    }

    @Override
    public int getVersion() {
        return m_version;
    }

    @Override
    public JsonNode save(final T spec, final JsonNodeFactory factory) {
        final ObjectNode root = factory.objectNode();
        root.put("type", m_transformIdentifier);
        if (m_version != 0) {
            root.put("version", m_version);
        }
        final JsonNode customConfig = saveInternal(spec, factory);
        if (customConfig != null) {
            root.set("config", customConfig);
        }
        return root;
    }

    protected abstract JsonNode saveInternal(final T spec, final JsonNodeFactory factory);

    @Override
    public T load(final JsonNode config) {
        final JsonNode customConfig = config.get("config");
        return loadInternal(customConfig);
    }

    protected abstract T loadInternal(final JsonNode config);
}
