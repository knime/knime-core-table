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
 *   Created on Jul 15, 2021 by Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
package org.knime.core.table.schema.traits;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("javadoc")
public class TraitsTest {
    @Test
    public void testDefaultTrait() {
        final var dt = new DefaultDataTraits();
        assertNull(dt.get(DataTrait.Type.DICT_ENCODING));

        final var et = DefaultDataTraits.EMPTY;
        assertNull(et.get(DataTrait.Type.DICT_ENCODING));

        final var dt2 = new DefaultDataTraits(new DictEncodingTrait(true));
        assertNotNull(dt2.get(DataTrait.Type.DICT_ENCODING));
        assertTrue(((DictEncodingTrait)dt2.get(DataTrait.Type.DICT_ENCODING)).isEnabled());
    }

    @Test
    public void testListTrait() {
        assertThrows(IllegalArgumentException.class, () -> new DefaultListDataTraits(null, null));

        final var lt2 = new DefaultListDataTraits(DefaultDataTraits.EMPTY);
        assertNull(lt2.get(DataTrait.Type.DICT_ENCODING));
        assertNull(lt2.getInner().get(DataTrait.Type.DICT_ENCODING));

        final var lt3 = new DefaultListDataTraits(new DataTrait[] { new DictEncodingTrait() }, DefaultDataTraits.EMPTY);
        assertNotNull(lt3.get(DataTrait.Type.DICT_ENCODING));
        assertNull(lt3.getInner().get(DataTrait.Type.DICT_ENCODING));

        final var lt4 = new DefaultListDataTraits(new DefaultDataTraits(new DictEncodingTrait()));
        assertNull(lt4.get(DataTrait.Type.DICT_ENCODING));
        assertNotNull(lt4.getInner().get(DataTrait.Type.DICT_ENCODING));
    }

    @Test
    public void testStructTrait() {
        assertThrows(IllegalArgumentException.class, () -> new DefaultStructDataTraits(null, null));

        final var st2 = new DefaultStructDataTraits(DefaultDataTraits.EMPTY);
        assertNull(st2.get(DataTrait.Type.DICT_ENCODING));
        assertEquals(st2.getInner().length, 1);
        assertNull(st2.getInner()[0].get(DataTrait.Type.DICT_ENCODING));

        final var st3 = new DefaultStructDataTraits(new DataTrait[] { new DictEncodingTrait() }, DefaultDataTraits.EMPTY);
        assertNotNull(st3.get(DataTrait.Type.DICT_ENCODING));
        assertNull(st3.getInner()[0].get(DataTrait.Type.DICT_ENCODING));

        final var st4 = new DefaultStructDataTraits(new DefaultDataTraits(new DictEncodingTrait()));
        assertNull(st4.get(DataTrait.Type.DICT_ENCODING));
        assertNotNull(st4.getInner()[0].get(DataTrait.Type.DICT_ENCODING));
    }
}
