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

/**
 * {@link DataTraits} for a struct, combining traits for the struct itself and the contained types
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public interface StructDataTraits extends DataTraits {

    /**
     * Get the {@link DataTrait} of the {@code i}-th data trait.
     *
     * @param i index of the data trait
     * @return get data trait of the {@code i}-th element
     */
    public DataTraits getDataTraits(final int i);

    /**
     * @return number of traits of this element
     */
    public int size();
}
