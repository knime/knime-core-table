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
 *   Created on Aug 9, 2023 by Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
package org.knime.core.table.virtual;

import java.io.IOException;

import org.knime.core.table.access.LongAccess.LongReadAccess;
import org.knime.core.table.cursor.LookaheadCursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.Selection;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.virtual.spec.MaskTransformSpec;

/**
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class MaskedRowAccessible implements LookaheadRowAccessible {

    private final LookaheadRowAccessible m_source;

    private final LookaheadRowAccessible m_mask;

    private final int m_maskColumn;

    MaskedRowAccessible(final LookaheadRowAccessible source, final LookaheadRowAccessible mask, final MaskTransformSpec spec) {
        m_source = source;
        m_mask = mask;
        m_maskColumn = spec.getMaskColumn();
        var maskSchema = mask.getSchema();
        if (!DataSpec.longSpec().equals(maskSchema.getSpec(m_maskColumn))) {
            throw new IllegalArgumentException("The mask column must be of type long.");
        }
    }

    @Override
    public ColumnarSchema getSchema() {
        return m_source.getSchema();
    }

    @Override
    public void close() throws IOException {
        m_source.close();
        m_mask.close();
    }

    @Override
    public LookaheadCursor<ReadAccessRow> createCursor() {
        return new MaskedLookaheadCursor(m_source.createCursor(), m_mask.createCursor(), m_maskColumn);
    }

    @Override
    public LookaheadCursor<ReadAccessRow> createCursor(final Selection selection) {
        return new MaskedLookaheadCursor(m_source.createCursor(selection), m_mask.createCursor(), m_maskColumn);
    }

    private static final class MaskedLookaheadCursor implements LookaheadCursor<ReadAccessRow> {

        private final LookaheadCursor<ReadAccessRow> m_maskCursor;

        private final LookaheadCursor<ReadAccessRow> m_sourceCursor;

        private long m_sourceIdx = -1;

        private final LongReadAccess m_maskAccess;

        MaskedLookaheadCursor(final LookaheadCursor<ReadAccessRow> sourceCursor,
            final LookaheadCursor<ReadAccessRow> maskCursor, final int maskColumn) {
            m_sourceCursor = sourceCursor;
            m_maskCursor = maskCursor;
            m_maskAccess = maskCursor.access().getAccess(maskColumn);
        }

        @Override
        public ReadAccessRow access() {
            return m_sourceCursor.access();
        }

        @Override
        public boolean forward() {
            if (!m_maskCursor.forward()) {
                return false;
            }
            // assumes that mask is ascending (something we could also check here)
            var nextIndex = m_maskAccess.getLongValue();
            // TODO use moveTo once random access is merged
            for (; m_sourceIdx < nextIndex; m_sourceIdx++) {
                // assumes that source and mask align (i.e. the indices in mask do not exceed the source)
                m_sourceCursor.forward();
            }
            return true;
        }

        @Override
        public void close() throws IOException {
            m_sourceCursor.close();
            m_maskCursor.close();
        }

        @Override
        public boolean canForward() {
            // assumes that source and mask align (i.e. the indices in mask do not exceed the source)
            return m_maskCursor.canForward();
        }

    }

}
