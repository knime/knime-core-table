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
package org.knime.core.table.virtual.spec;

/**
 * TransformSpec for a mask operation.
 * Stores the index of the column that holds the mask in the mask table
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class MaskTransformSpec implements TableTransformSpec {

    private final int m_maskColumn;

    public MaskTransformSpec(final int maskColumn) {
        m_maskColumn = maskColumn;
    }

    public int getMaskColumn() {
        return m_maskColumn;
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        } else if (obj instanceof MaskTransformSpec other) {
            return m_maskColumn == other.m_maskColumn;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(m_maskColumn);
    }
}
