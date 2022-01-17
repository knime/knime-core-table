package org.knime.core.table.virtual.graph.rag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Maintains a set of {@code List<O>}s:
 * <ul>
 *     <li>one list containing all the objects</li>
 *     <li>one list for each type {@code T}, containing all objects of that type.</li>
 * </ul>
 * This is used in {@link RagGraph}, for example to store different types of edges.
 *
 * @param <T> enumerates all possible object types
 * @param <O> objects to store in the map where each object knows its {@code T type()}.
 */
class TypedObjects<T extends Enum<T>, O extends Typed<T>> {

    private final List<List<O>> objects;

    private final List<List<O>> unmodifiableObjects;

    public TypedObjects(Class<T> type) {
        final T[] types = type.getEnumConstants();
        final int n = types.length + 1;
        objects = new ArrayList<>(n);
        unmodifiableObjects = new ArrayList<>(n);
        for (int i = 0; i < n; ++i) {
            final ArrayList<O> list = new ArrayList<>();
            objects.add(list);
            unmodifiableObjects.add(Collections.unmodifiableList(list));
        }
    }

    /**
     * Add {@code object} to the all-objects list, and to the list for the object's
     * {@link Typed#type()}.
     */
    public boolean add(O object) {
        objects.get(object.type().ordinal() + 1).add(object);
        return objects.get(0).add(object);
    }

    public void remove(O object) {
        if (objects.get(object.type().ordinal() + 1).remove(object))
            objects.get(0).remove(object);
    }

    public List<O> unmodifiable() {
        return unmodifiable(null);
    }

    public List<O> unmodifiable(T type) {
        return type == null ? unmodifiableObjects.get(0) : unmodifiableObjects.get(type.ordinal() + 1);
    }
}
