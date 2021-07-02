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
 * A single piece of additional information about a {@link DataSpec}.
 *
 * To quote Bjarne Stroustrup (C++): Think of a trait as a small object whose main purpose is to carry
 * information used by another object or algorithm to determine "policy" or "implementation details".
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public interface DataTrait {

    /**
     * A list of available DataTrait types, each describing a piece of information about a {@link DataSpec}
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    public enum Type {
        /**
         * The dictionary encoding trait: {@link DictEncodingTrait}
         */
        DICT_ENCODING;
    }

    /**
     * @return the type of this DataTrait
     */
    Type getType();
}
