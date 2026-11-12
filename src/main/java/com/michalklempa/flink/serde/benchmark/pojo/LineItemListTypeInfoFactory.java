package com.michalklempa.flink.serde.benchmark.pojo;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

public class LineItemListTypeInfoFactory extends TypeInfoFactory<List<LineItem>> {
    @Override
    public TypeInformation<List<LineItem>> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
        return Types.LIST(Types.POJO(LineItem.class));
    }
}
