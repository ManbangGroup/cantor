package io.cantor.http;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * A copy of jackson TypeReference
 */
public abstract class TypeRef<T> implements Comparable<TypeRef<T>> {
    protected final Type _type;

    protected TypeRef() {
        Type superClass = getClass().getGenericSuperclass();
        if (superClass instanceof Class<?>) {
            throw new IllegalArgumentException("Internal error: TypeRef constructed without actual type information");
        }
        _type = ((ParameterizedType) superClass).getActualTypeArguments()[0];
    }

    public Type getType() { return _type; }

    @Override
    public int compareTo(TypeRef<T> o) { return 0; }
}
