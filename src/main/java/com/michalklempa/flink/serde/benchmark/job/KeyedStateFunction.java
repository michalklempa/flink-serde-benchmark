package com.michalklempa.flink.serde.benchmark.job;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class KeyedStateFunction<T> extends KeyedProcessFunction<Long, Tuple2<Long, T>, T> {
    private final TypeInformation<T> typeInfo;
    private transient ValueState<T> state;

    public KeyedStateFunction(TypeInformation<T> typeInfo) {
        this.typeInfo = typeInfo;
    }

    @Override
    public void open(Configuration parameters) {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("order", typeInfo));
    }

    @Override
    public void processElement(Tuple2<Long, T> value, Context ctx, Collector<T> out) throws Exception {
        T previous = state.value();
        state.update(value.f1);
        out.collect(previous != null ? previous : value.f1);
    }
}
