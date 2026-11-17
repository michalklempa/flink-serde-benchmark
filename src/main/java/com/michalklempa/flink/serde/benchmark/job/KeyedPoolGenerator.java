package com.michalklempa.flink.serde.benchmark.job;

import com.michalklempa.flink.serde.benchmark.data.Profile;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

public class KeyedPoolGenerator<T> implements GeneratorFunction<Long, Tuple2<Long, T>> {
    private final PoolGenerator<T> pool;
    private final long keys;
    private final java.util.Random random = new java.util.Random(12345L);

    public KeyedPoolGenerator(String variantName, Profile profile, long keys) {
        this.pool = new PoolGenerator<>(variantName, profile);
        this.keys = keys;
    }

    @Override
    public void open(org.apache.flink.api.connector.source.SourceReaderContext readerContext) {
        pool.open(readerContext);
    }

    @Override
    public Tuple2<Long, T> map(Long index) {
        return Tuple2.of((long) (keys * Math.pow(random.nextDouble(), 3)), pool.map(index));
    }
}
