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
 */
package org.knime.core.table.virtual.graph.rag;

import org.knime.core.table.virtual.graph.rag3.debug.ConsumerTransformSpec;
import org.knime.core.table.virtual.spec.AppendMissingValuesTransformSpec;
import org.knime.core.table.virtual.spec.AppendTransformSpec;
import org.knime.core.table.virtual.spec.ObserverTransformSpec;
import org.knime.core.table.virtual.spec.RowIndexTransformSpec;
import org.knime.core.table.virtual.spec.SelectColumnsTransformSpec;
import org.knime.core.table.virtual.spec.ConcatenateTransformSpec;
import org.knime.core.table.virtual.spec.IdentityTransformSpec;
import org.knime.core.table.virtual.spec.MapTransformSpec;
import org.knime.core.table.virtual.spec.MaterializeTransformSpec;
import org.knime.core.table.virtual.spec.RowFilterTransformSpec;
import org.knime.core.table.virtual.spec.SliceTransformSpec;
import org.knime.core.table.virtual.spec.SourceTransformSpec;
import org.knime.core.table.virtual.spec.TableTransformSpec;

public enum RagNodeType {
    SOURCE, //
    SLICE, //
    APPEND, //
    APPENDMISSING, //
    MISSING, //
    CONCATENATE, //
    COLFILTER, // TODO (TP) rename COLSELECT
    MAP, //
    ROWFILTER, //
    CONSUMER, //
    MATERIALIZE, //
    WRAPPER, //
    IDENTITY, //
    ROWINDEX, //
    OBSERVER;

    public static RagNodeType forSpec(final TableTransformSpec spec) {
        if (spec instanceof SourceTransformSpec)
            return RagNodeType.SOURCE;
        else if (spec instanceof MissingValuesSourceTransformSpec)
            return RagNodeType.MISSING;
        else if (spec instanceof SliceTransformSpec)
            return RagNodeType.SLICE;
        else if (spec instanceof SelectColumnsTransformSpec)
            return RagNodeType.COLFILTER;
        else if (spec instanceof AppendMissingValuesTransformSpec)
            return RagNodeType.APPENDMISSING;
        else if (spec instanceof AppendTransformSpec)
            return RagNodeType.APPEND;
        else if (spec instanceof ConcatenateTransformSpec)
            return RagNodeType.CONCATENATE;
        else if (spec instanceof MapTransformSpec)
            return RagNodeType.MAP;
        else if (spec instanceof RowFilterTransformSpec)
            return RagNodeType.ROWFILTER;
        else if (spec instanceof ConsumerTransformSpec)
            return RagNodeType.CONSUMER;
        else if (spec instanceof MaterializeTransformSpec)
            return RagNodeType.MATERIALIZE;
        else if (spec instanceof WrapperTransformSpec)
            return RagNodeType.WRAPPER;
        else if (spec instanceof IdentityTransformSpec)
            return RagNodeType.IDENTITY;
        else if (spec instanceof RowIndexTransformSpec)
            return RagNodeType.ROWINDEX;
        else if (spec instanceof ObserverTransformSpec)
            return RagNodeType.OBSERVER;
        else
            throw new IllegalArgumentException();
    }
}
