package com.michalklempa.flink.serde.benchmark.job;

import com.michalklempa.flink.serde.benchmark.serde.SerializerVariant;
import com.michalklempa.flink.serde.benchmark.serde.Variants;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public final class JobRunner {
    private JobRunner() {
    }

    public static void main(String[] args) throws Exception {
        JobResult result = run(JobConfig.parse(args));
        System.out.println(result.toLine());
    }

    @SuppressWarnings("unchecked")
    public static JobResult run(JobConfig config) throws Exception {
        return runTyped(config, (SerializerVariant<Object>) Variants.byName(config.variant));
    }

    private static <T> JobResult runTyped(JobConfig config, SerializerVariant<T> variant) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        env.setMaxParallelism(128);
        env.disableOperatorChaining();
        if (!variant.usesKryo()) {
            env.getConfig().disableGenericTypes();
        }
        TypeInformation<T> typeInfo = variant.typeInfo();
        DataStream<T> out;
        if (config.kind == JobConfig.Kind.A) {
            DataGeneratorSource<T> source = new DataGeneratorSource<>(
                    new PoolGenerator<T>(config.variant, config.profile), config.records, typeInfo);
            out = env.fromSource(source, WatermarkStrategy.noWatermarks(), "source")
                    .map(x -> x).returns(typeInfo).name("pass-through");
        } else {
            env.setStateBackend(new EmbeddedRocksDBStateBackend());
            TypeInformation<Tuple2<Long, T>> tupleInfo = Types.TUPLE(Types.LONG, typeInfo);
            DataGeneratorSource<Tuple2<Long, T>> source = new DataGeneratorSource<>(
                    new KeyedPoolGenerator<T>(config.variant, config.profile, config.keys), config.records,
                    tupleInfo);
            out = env.fromSource(source, WatermarkStrategy.noWatermarks(), "source")
                    .keyBy(t -> t.f0, Types.LONG)
                    .process(new KeyedStateFunction<>(typeInfo), typeInfo).name("keyed-state");
        }
        out.addSink(new TimingSink<T>(config.warmup, config.records)).name("timing-sink");
        JobExecutionResult result = env.execute(config.kind + "-" + config.variant);
        long start = result.getAccumulatorResult(TimingSink.START);
        long end = result.getAccumulatorResult(TimingSink.END);
        long count = result.getAccumulatorResult(TimingSink.COUNT);
        if (count != config.records) {
            throw new IllegalStateException("expected " + config.records + " records, sink saw " + count);
        }
        return new JobResult(config, config.records - config.warmup, end - start);
    }
}
