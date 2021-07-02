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
 *   Created on Jul 13, 2021 by Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
package org.knime.core.table.schema.traits;

import org.knime.core.table.schema.DataSpec;

/**
 * If the {@link DictEncodingTrait} is provided alongside a {@link DataSpec},
 * that means the data should be stored using dictionary encoding.
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public class DictEncodingTrait implements DataTrait {
    private final boolean m_enabled;

    /**
     * Create a dictionary encoding trait, but leave it disabled
     */
    public DictEncodingTrait() {
        this(false);
    }

    /**
     * Create a dictionary encoding trait and possibly enable it
     * @param enabled Whether dictionary encoding should be enabled
     */
    public DictEncodingTrait(final boolean enabled) {
        m_enabled = enabled;
    }

    /**
     * @return whether dictionary encoding is enabled
     */
    public boolean isEnabled() {
        return m_enabled;
    }

    @Override
    public Type getType() {
        return DataTrait.Type.DICT_ENCODING;
    }
}
