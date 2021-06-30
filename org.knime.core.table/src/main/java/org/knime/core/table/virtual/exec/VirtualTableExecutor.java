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
 *   Created on May 26, 2021 by dietzc
 */
package org.knime.core.table.virtual.exec;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.knime.core.table.row.RowAccessible;

/**
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @since 4.4
 */
public interface VirtualTableExecutor {

    List<RowAccessible> execute(Map<UUID, RowAccessible> inputs);
}
