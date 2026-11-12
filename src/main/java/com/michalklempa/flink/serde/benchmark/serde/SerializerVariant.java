package com.michalklempa.flink.serde.benchmark.serde;

import com.michalklempa.flink.serde.benchmark.data.OrderSpec;
import java.util.function.Function;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

public final class SerializerVariant<T> {
    private final String name;
    private final TypeInformation<T> typeInfo;
    private final Function<OrderSpec, T> mapper;
    private final Class<?> expectedSerializer;
    private final boolean usesKryo;

    SerializerVariant(String name, TypeInformation<T> typeInfo, Function<OrderSpec, T> mapper,
                      Class<?> expectedSerializer, boolean usesKryo) {
        this.name = name;
        this.typeInfo = typeInfo;
        this.mapper = mapper;
        this.expectedSerializer = expectedSerializer;
        this.usesKryo = usesKryo;
    }

    public String name() {
        return name;
    }

    public TypeInformation<T> typeInfo() {
        return typeInfo;
    }

    public T map(OrderSpec spec) {
        return mapper.apply(spec);
    }

    public Class<?> expectedSerializer() {
        return expectedSerializer;
    }

    public boolean usesKryo() {
        return usesKryo;
    }

    public TypeSerializer<T> createSerializer(boolean genericTypesDisabled) {
        ExecutionConfig config = new ExecutionConfig();
        if (genericTypesDisabled) {
            config.disableGenericTypes();
        }
        return typeInfo.createSerializer(config);
    }

    @Override
    public String toString() {
        return name;
    }
}
