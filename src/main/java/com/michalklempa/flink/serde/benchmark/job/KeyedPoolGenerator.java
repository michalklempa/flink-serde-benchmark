package com.michalklempa.flink.serde.benchmark.job;

import com.michalklempa.flink.serde.benchmark.data.Profile;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

public class KeyedPoolGenerator<T> implements GeneratorFunction<Long, Tuple2<Long, T>> {
    private final PoolGenerator<T> pool;
    private final long keys;

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
        return Tuple2.of(index % keys, pool.map(index));
    }
}
