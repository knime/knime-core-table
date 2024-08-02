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
 *   May 22, 2024 (benjamin): created
 */
package org.knime.core.expressions;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A container for the arguments of a function or aggregation. Supports named and variable arguments. For getting
 * Arguments that follows the function signature, use the static method {@link #matchSignature(List, List, Map)}. This
 * will return an error message if the arguments do not match the signature. If the generic type is {@link ValueType},
 * the method {@link #matchTypes(List, Arguments)} can be used to check if the arguments are of the expected type.
 *
 * @param <T> the type of the arguments
 * @param m_namedArguments a map of named arguments
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
public class Arguments<T> {

    private static final String MISSING_REQUIRED_ARGUMENT = "Missing required argument: ";

    // A LinkedHasmap is used to preserve the order of the arguments from the input order.
    // This will be used to allow for a conversion to a list of arguments in the same order as
    // they were input which match the order of the function signature when Arguments are created
    // by matchSignature.
    private final LinkedHashMap<String, T> m_namedArguments;

    private final List<T> m_varArgument;

    private final String m_varArgumentName;

    /**
     * @param namedArguments
     * @param varArgument
     * @param varArgumentName
     */
    public Arguments(final LinkedHashMap<String, T> namedArguments, final List<T> varArgument,
        final String varArgumentName) {

        this.m_namedArguments = namedArguments;
        this.m_varArgument = varArgument;
        this.m_varArgumentName = varArgumentName;

    }

    /**
     * @param namedArguments
     * @param varArgument
     */
    public Arguments(final LinkedHashMap<String, T> namedArguments, final List<T> varArgument) {

        this(namedArguments, varArgument, "defaultVarArgumentName");

    }

    /**
     * @param namedArguments
     */
    public Arguments(final LinkedHashMap<String, T> namedArguments) {
        this(namedArguments, List.of());
    }

    /**
     * Create an empty Arguments object.
     *
     * @param <T>
     * @return an empty Arguments
     */
    public static <T> Arguments<T> empty() {
        var emptyMap = new LinkedHashMap<String, T>();
        return new Arguments<>(emptyMap);
    }

    /**
     * Match the positional and named arguments to the signature. Canonical way to create proper Arguments from input
     * parameters. If the arguments do not match the signature, return an error message.
     *
     * @param <T> the type of the arguments
     * @param signature the signature of the function
     * @param positionalArguments the positional arguments to match against the signature (in order)
     * @param namedArguments the named arguments to match against the signature (by name)
     * @return the matched arguments or an error message if the arguments do not match the signature
     */
    public static <T> ReturnResult<Arguments<T>> matchSignature(final List<OperatorDescription.Argument> signature,
        final List<T> positionalArguments, final Map<String, T> namedArguments) {

        var isLastArgumentVariable = !signature.isEmpty() ? signature.get(signature.size() - 1).isVariable() : false;

        var argumentsMap = new LinkedHashMap<String, T>();

        // resolving the positional arguments
        for (var i = 0; i < positionalArguments.size(); i++) {

            if (i >= signature.size() - 1 && isLastArgumentVariable) {
                break;
            }
            if (i >= signature.size()) {
                return ReturnResult.failure("Too many arguments. Expected at most " + signature.size() + " arguments.");
            }
            argumentsMap.put(signature.get(i).name(), positionalArguments.get(i));
        }

        // resolving the named arguments
        var validArgumentIds = signature.stream().map(OperatorDescription.Argument::name).collect(Collectors.toSet());
        for (final var entry : namedArguments.entrySet()) {
            var name = entry.getKey();
            if (argumentsMap.containsKey(name)) {
                return ReturnResult.failure("Argument '" + name + "' is already set.");
            }
            if (!validArgumentIds.contains(name)) {
                return ReturnResult
                    .failure("No argument with identifier '" + name + "' found in the function signature.");
            }
            argumentsMap.put(name, entry.getValue());
        }

        // resolving the variable arguments
        var varargs = new ArrayList<T>();
        String varargName = null;
        if (!positionalArguments.isEmpty() && isLastArgumentVariable) {
            varargName = signature.get(signature.size() - 1).name();

            for (var i = signature.size() - 1; i < positionalArguments.size(); i++) {
                varargs.add(positionalArguments.get(i));
            }
        }

        // check if all required arguments are present
        for (var arg : signature) {
            if (!arg.isOptional() && !arg.isVariable() && !argumentsMap.containsKey(arg.name())) {
                return ReturnResult.failure(MISSING_REQUIRED_ARGUMENT + arg.name() + "." + argumentsMap);
            }
        }

        return ReturnResult.success(
            varargName == null ? new Arguments<>(argumentsMap) : new Arguments<>(argumentsMap, varargs, varargName));
    }

    /**
     * convenience function to match the signature without named arguments.
     *
     * @param <T>
     * @param signature
     * @param positionalArguments
     * @return the matched arguments or an error message if the arguments do not match the signature
     */
    public static <T> ReturnResult<Arguments<T>> matchSignature(final List<OperatorDescription.Argument> signature,
        final List<T> positionalArguments) {
        return matchSignature(signature, positionalArguments, Map.of());
    }

    /**
     * Check if the arguments match the types of the signature.
     *
     * @param signature the signature of the function
     * @param arguments the arguments to check
     * @return true if the arguments match the signature, false otherwise
     */
    public static ReturnResult<Boolean> matchTypes(final List<OperatorDescription.Argument> signature,
        final Arguments<ValueType> arguments) {
        var test = arguments.getNamedArguments();

        for (var arg : test.entrySet()) {

            OperatorDescription.Argument argFunc = null;
            for (var a : signature) {
                if (a.name().equals(arg.getKey())) {
                    argFunc = a;
                    break;
                }
            }

            if (argFunc == null) {
                return ReturnResult.failure("Argument not found: " + arg.getKey());
            }

            if (!argFunc.matcher().matches(arg.getValue())) {

                return ReturnResult.failure("Argument '" + arg.getKey() + "' is not of the expected type: "
                    + argFunc.type() + " but got " + arg.getValue() + ".");
            }
        }

        return ReturnResult.success(true);
    }

    /**
     * Get an argument by name. If the argument is missing, return an error.
     *
     * @param name
     * @return the argument or an error message if the argument is missing
     */
    public ReturnResult<T> getArgument(final String name) {
        var argument = m_namedArguments.get(name);
        if (argument == null) {
            return ReturnResult.failure("Argument not found: " + name);
        }
        return ReturnResult.success(argument);
    }

    /**
     * Get the variable argument. If the variable argument is missing, return an error. There is at most one variable
     * argument.
     *
     * @return the variable argument or an error message if the variable argument is missing
     */
    public ReturnResult<List<T>> getVariableArgument() {
        if (m_varArgument.isEmpty()) {
            return ReturnResult.failure("No variable argument present");
        }

        return ReturnResult.success(m_varArgument);
    }

    /**
     * @return all named arguments
     */
    public Map<String, T> getNamedArguments() {
        return m_namedArguments;
    }

    /**
     * create a list of all arguments in the order they were input which should match the order of the function
     * signature.
     *
     * @return a list of arguments
     */
    public List<T> toList() {
        List<T> list = new ArrayList<>(m_namedArguments.values());
        list.addAll(m_varArgument);
        return list;
    }

    /**
     * Allow to conveniently stream over all arguments.
     *
     * @return a stream of all arguments
     */
    public Stream<T> stream() {
        return this.toList().stream();
    }

    /**
     * @return the number of arguments stored in this object
     */
    public Integer size() {
        return m_namedArguments.size() + m_varArgument.size();
    }

    /**
     * Check if any argument matches the predicate.
     *
     * @param predicate the predicate to test
     * @return true if any argument matches, false otherwise
     */
    public boolean anyMatch(final Predicate<? super T> predicate) {
        for (T value : m_namedArguments.values()) {
            if (predicate.test(value)) {
                return true;
            }
        }
        for (T value : m_varArgument) {
            if (predicate.test(value)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check if all arguments match the predicate
     *
     * @param predicate
     * @return true if all arguments match the predicate, false otherwise
     */
    public boolean allMatch(final Predicate<? super T> predicate) {
        for (T value : m_namedArguments.values()) {
            if (!predicate.test(value)) {
                return false;
            }
        }
        for (T value : m_varArgument) {
            if (!predicate.test(value)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Applies a mapping function to all arguments.
     *
     * @param <O> the type of the mapped arguments
     * @param mapper the mapping function
     * @return a new {@link Arguments} object with the mapped arguments
     */
    public <O> Arguments<O> map(final Function<T, O> mapper) {

        LinkedHashMap<String, O> mappedNamedArguments = this.m_namedArguments.entrySet().stream()
            .map(entry -> new SimpleEntry<>(entry.getKey(), mapper.apply(entry.getValue())))
            .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

        List<O> mappedVarArgument = m_varArgument.stream().map(mapper).collect(Collectors.toList());

        return new Arguments<>(mappedNamedArguments, mappedVarArgument);
    }

    /**
     * Renders the arguments as a string.
     *
     * @param argRenderer a function that renders an individual argument
     * @return a string representation of the arguments
     */
    public String renderArgumentList(final Function<T, String> argRenderer) {
        var renderedNamedArguments = m_namedArguments.entrySet().stream()
            .map(entry -> entry.getKey() + "=" + argRenderer.apply(entry.getValue())).collect(Collectors.joining(", "));

        var renderedVariableArguments =
            m_varArgumentName + "=[" + m_varArgument.stream().map(argRenderer).collect(Collectors.joining(", ")) + "]";

        return "(" + renderedNamedArguments + (m_varArgument.isEmpty() ? "" : (", " + renderedVariableArguments)) + ")";

    }

    @Override
    public String toString() {
        return renderArgumentList(Object::toString);
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Arguments<?> arguments = (Arguments<?>)obj;
        return m_namedArguments.equals(arguments.m_namedArguments) && m_varArgument.equals(arguments.m_varArgument)
            && m_varArgumentName.equals(arguments.m_varArgumentName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return super.hashCode();
    }

}
