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
 *   Jan 31, 2025 (benjamin): created
 */
package org.knime.core.expressions;

/**
 * TODO javadoc
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
abstract class ContextAwareComputer<C extends Computer> implements Computer {

    final C m_delegate;

    final TextRange m_textLocation;

    public static Computer wrap(final Computer delegate, final TextRange textLocation) {
        if (delegate instanceof BooleanComputer c) {
            return new ContextAwareBooleanComputer(c, textLocation);
        } else if (delegate instanceof IntegerComputer c) {
            return new ContextAwareIntegerComputer(c, textLocation);
        } else if (delegate instanceof FloatComputer c) {
            return new ContextAwareFloatComputer(c, textLocation);
        } else if (delegate instanceof StringComputer c) {
            return new ContextAwareStringComputer(c, textLocation);
        }
        throw new IllegalArgumentException("Unsupported computer type: " + delegate.getClass());
    }

    ContextAwareComputer(final C delegate, final TextRange textLocation) {
        m_delegate = delegate;
        m_textLocation = textLocation;
    }

    @Override
    public boolean isMissing(final EvaluationContext ctx) throws ExpressionEvaluationException {
        try {
            return m_delegate.isMissing(ctx);
        } catch (final ExpressionEvaluationException e) {
            e.addLocation(m_textLocation);
            throw e;
        }
    }

    static final class ContextAwareBooleanComputer extends ContextAwareComputer<BooleanComputer>
        implements BooleanComputer {

        private ContextAwareBooleanComputer(final BooleanComputer delegate, final TextRange textLocation) {
            super(delegate, textLocation);
        }

        @Override
        public boolean compute(final EvaluationContext ctx) throws ExpressionEvaluationException {
            try {
                return m_delegate.compute(ctx);
            } catch (final ExpressionEvaluationException e) {
                e.addLocation(m_textLocation);
                throw e;
            }
        }
    }

    static final class ContextAwareIntegerComputer extends ContextAwareComputer<IntegerComputer>
        implements IntegerComputer {

        private ContextAwareIntegerComputer(final IntegerComputer delegate, final TextRange textLocation) {
            super(delegate, textLocation);
        }

        @Override
        public long compute(final EvaluationContext ctx) throws ExpressionEvaluationException {
            try {
                return m_delegate.compute(ctx);
            } catch (final ExpressionEvaluationException e) {
                e.addLocation(m_textLocation);
                throw e;
            }
        }
    }

    static final class ContextAwareFloatComputer extends ContextAwareComputer<FloatComputer> implements FloatComputer {

        private ContextAwareFloatComputer(final FloatComputer delegate, final TextRange textLocation) {
            super(delegate, textLocation);
        }

        @Override
        public double compute(final EvaluationContext ctx) throws ExpressionEvaluationException {
            try {
                return m_delegate.compute(ctx);
            } catch (final ExpressionEvaluationException e) {
                e.addLocation(m_textLocation);
                throw e;
            }
        }
    }

    static final class ContextAwareStringComputer extends ContextAwareComputer<StringComputer>
        implements StringComputer {

        private ContextAwareStringComputer(final StringComputer delegate, final TextRange textLocation) {
            super(delegate, textLocation);
        }

        @Override
        public String compute(final EvaluationContext ctx) throws ExpressionEvaluationException {
            try {
                return m_delegate.compute(ctx);
            } catch (final ExpressionEvaluationException e) {
                e.addLocation(m_textLocation);
                throw e;
            }
        }
    }
}
