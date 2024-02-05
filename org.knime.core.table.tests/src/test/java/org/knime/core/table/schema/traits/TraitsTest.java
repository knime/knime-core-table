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
 *   Created on Jul 15, 2021 by Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
package org.knime.core.table.schema.traits;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.knime.core.table.schema.traits.DataTrait.DictEncodingTrait;
import org.knime.core.table.schema.traits.DataTrait.DictEncodingTrait.KeyType;

/**
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("javadoc")
public class TraitsTest {
    @Test
    public void testDefaultTrait() {
        final var dt = new DefaultDataTraits();
        assertNull(dt.get(DictEncodingTrait.class));

        final var et = DefaultDataTraits.EMPTY;
        assertNull(et.get(DictEncodingTrait.class));

        final var dt2 = new DefaultDataTraits(new DictEncodingTrait());
        assertNotNull(dt2.get(DictEncodingTrait.class));
        assertTrue(DictEncodingTrait.isEnabled(dt2));
    }

    @Test
    public void testDictEncodingKeyTypeByte() {
        final var dt = new DefaultDataTraits(new DictEncodingTrait(KeyType.BYTE_KEY));
        assertNotNull(dt.get(DictEncodingTrait.class));
        assertTrue(DictEncodingTrait.isEnabled(dt));
        assertEquals(KeyType.BYTE_KEY, DictEncodingTrait.keyType(dt));
    }

    @Test
    public void testDictEncodingKeyTypeInt() {
        final var dt = new DefaultDataTraits(new DictEncodingTrait(KeyType.INT_KEY));
        assertNotNull(dt.get(DictEncodingTrait.class));
        assertTrue(DictEncodingTrait.isEnabled(dt));
        assertEquals(KeyType.INT_KEY, DictEncodingTrait.keyType(dt));
    }

    @Test
    public void testDictEncodingKeyTypeLong() {
        final var dt = new DefaultDataTraits(new DictEncodingTrait(KeyType.LONG_KEY));
        assertNotNull(dt.get(DictEncodingTrait.class));
        assertTrue(DictEncodingTrait.isEnabled(dt));
        assertEquals(KeyType.LONG_KEY, DictEncodingTrait.keyType(dt));
    }

    @Test
    public void testDictEncodingKeyTypeDefault() {
        final var dt = new DefaultDataTraits(new DictEncodingTrait());
        assertNotNull(dt.get(DictEncodingTrait.class));
        assertTrue(DictEncodingTrait.isEnabled(dt));
        assertEquals(KeyType.LONG_KEY, DictEncodingTrait.keyType(dt));
    }

    @Test
    public void testListTrait() {
        assertThrows(IllegalArgumentException.class, () -> new DefaultListDataTraits(null, null));

        final var lt2 = new DefaultListDataTraits(DefaultDataTraits.EMPTY);
        assertNull(lt2.get(DictEncodingTrait.class));
        assertNull(lt2.getInner().get(DictEncodingTrait.class));

        final var lt3 = new DefaultListDataTraits(new DataTrait[] { new DictEncodingTrait() }, DefaultDataTraits.EMPTY);
        assertNotNull(lt3.get(DictEncodingTrait.class));
        assertNull(lt3.getInner().get(DictEncodingTrait.class));

        final var lt4 = new DefaultListDataTraits(new DefaultDataTraits(new DictEncodingTrait()));
        assertNull(lt4.get(DictEncodingTrait.class));
        assertNotNull(lt4.getInner().get(DictEncodingTrait.class));
    }

    @Test
    public void testStructTrait() {
        assertThrows(IllegalArgumentException.class, () -> new DefaultStructDataTraits(null, (DataTraits[])null));

        final var st2 = new DefaultStructDataTraits(DefaultDataTraits.EMPTY);
        assertNull(st2.get(DictEncodingTrait.class));
        assertEquals(st2.size(), 1);
        assertNull(st2.getDataTraits(0).get(DictEncodingTrait.class));

        final var st3 = new DefaultStructDataTraits(new DataTrait[] { new DictEncodingTrait() }, DefaultDataTraits.EMPTY);
        assertNotNull(st3.get(DictEncodingTrait.class));
        assertNull(st3.getDataTraits(0).get(DictEncodingTrait.class));

        final var st4 = new DefaultStructDataTraits(new DefaultDataTraits(new DictEncodingTrait()));
        assertNull(st4.get(DictEncodingTrait.class));
        assertNotNull(st4.getDataTraits(0).get(DictEncodingTrait.class));
    }
}
