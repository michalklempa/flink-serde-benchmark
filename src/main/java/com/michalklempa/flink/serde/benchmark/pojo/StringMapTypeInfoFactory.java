package com.michalklempa.flink.serde.benchmark.pojo;

import java.lang.reflect.Type;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

public class StringMapTypeInfoFactory extends TypeInfoFactory<Map<String, String>> {
    @Override
    public TypeInformation<Map<String, String>> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
        return Types.MAP(Types.STRING, Types.STRING);
    }
}
