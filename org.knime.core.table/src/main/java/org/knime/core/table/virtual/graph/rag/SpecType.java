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
 *   3 Dec 2024 (pietzsch): created
 */
package org.knime.core.table.virtual.graph.rag;

import org.knime.core.table.virtual.spec.AppendMapTransformSpec;
import org.knime.core.table.virtual.spec.AppendMissingValuesTransformSpec;
import org.knime.core.table.virtual.spec.AppendTransformSpec;
import org.knime.core.table.virtual.spec.ConcatenateTransformSpec;
import org.knime.core.table.virtual.spec.MapTransformSpec;
import org.knime.core.table.virtual.spec.ObserverTransformSpec;
import org.knime.core.table.virtual.spec.RowFilterTransformSpec;
import org.knime.core.table.virtual.spec.RowIndexTransformSpec;
import org.knime.core.table.virtual.spec.SelectColumnsTransformSpec;
import org.knime.core.table.virtual.spec.SliceTransformSpec;
import org.knime.core.table.virtual.spec.SourceTransformSpec;
import org.knime.core.table.virtual.spec.TableTransformSpec;


// TODO (TP):
//  COLSELECT, APPENDMISSING, APPENDMAP never make it into the TableTransformGraph.
//  Should we split SpecType and NodeType? RawSpecType, NodeSpecType, ... ?
public enum SpecType {
    SOURCE, //
    SLICE, //
    APPEND, //
    APPENDMAP, //
    APPENDMISSING, //
    CONCATENATE, //
    COLSELECT, //
    MAP, //
    ROWFILTER, //
    ROWINDEX, //
    OBSERVER;

    public static SpecType forSpec(final TableTransformSpec spec) { // NOSONAR This method is not too complex...
        if (spec instanceof SourceTransformSpec) {
            return SOURCE;
        } else if (spec instanceof SliceTransformSpec) {
            return SLICE;
        } else if (spec instanceof SelectColumnsTransformSpec) {
            return COLSELECT;
        } else if (spec instanceof AppendTransformSpec) {
            return APPEND;
        } else if (spec instanceof AppendMissingValuesTransformSpec) {
            return APPENDMISSING;
        } else if (spec instanceof AppendMapTransformSpec) {
            return APPENDMAP;
        } else if (spec instanceof ConcatenateTransformSpec) {
            return CONCATENATE;
        } else if (spec instanceof MapTransformSpec) {
            return MAP;
        } else if (spec instanceof RowFilterTransformSpec) {
            return ROWFILTER;
        } else if (spec instanceof RowIndexTransformSpec) {
            return ROWINDEX;
        } else if (spec instanceof ObserverTransformSpec) {
            return OBSERVER;
        } else {
            throw new IllegalArgumentException("TableTransformSpec " + spec + ": spec type not handled (yet)");
        }
    }
}
