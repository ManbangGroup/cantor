package io.cantor.http;

import com.fasterxml.jackson.core.type.TypeReference;

import java.lang.reflect.Type;

public class JacksonTypeRefs {
    public static <T> TypeReference<T> typeRef(TypeRef<T> typeRef) {
        return new TypeReference<T>() {
            @Override
            public Type getType() {
                return typeRef.getType();
            }
        };
    }
}
