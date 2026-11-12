package com.michalklempa.flink.serde.benchmark.job;

import com.michalklempa.flink.serde.benchmark.bench.BenchCase;
import com.michalklempa.flink.serde.benchmark.data.OrderGenerator;
import com.michalklempa.flink.serde.benchmark.data.Profile;
import com.michalklempa.flink.serde.benchmark.serde.SerializerVariant;
import com.michalklempa.flink.serde.benchmark.serde.Variants;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

public class PoolGenerator<T> implements GeneratorFunction<Long, T> {
    private final String variantName;
    private final Profile profile;
    private transient Object[] pool;
    private transient int mask;

    public PoolGenerator(String variantName, Profile profile) {
        this.variantName = variantName;
        this.profile = profile;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void open(SourceReaderContext readerContext) {
        SerializerVariant<T> variant = (SerializerVariant<T>) Variants.byName(variantName);
        int size = BenchCase.poolSize(profile);
        pool = new Object[size];
        mask = size - 1;
        OrderGenerator generator = new OrderGenerator(12345L, profile);
        for (int i = 0; i < size; i++) {
            pool[i] = variant.map(generator.next());
        }
    }

    @SuppressWarnings("unchecked")
    protected T at(long index) {
        return (T) pool[(int) (index & mask)];
    }

    @Override
    public T map(Long index) {
        return at(index);
    }
}
