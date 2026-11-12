package com.michalklempa.flink.serde.benchmark.pojo;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

public class StringListTypeInfoFactory extends TypeInfoFactory<List<String>> {
    @Override
    public TypeInformation<List<String>> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
        return Types.LIST(Types.STRING);
    }
}
