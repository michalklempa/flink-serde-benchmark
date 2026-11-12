package com.michalklempa.flink.serde.benchmark.bench;

import com.michalklempa.flink.serde.benchmark.data.OrderGenerator;
import com.michalklempa.flink.serde.benchmark.data.Profile;
import com.michalklempa.flink.serde.benchmark.serde.SerializerVariant;
import java.io.IOException;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

public final class BenchCase<T> {
    private final SerializerVariant<T> variant;
    private final TypeSerializer<T> serializer;
    private final int pool;
    private final Object[] objects;
    private final byte[][] bytes;
    private T reuse;
    private final DataOutputSerializer out = new DataOutputSerializer(1 << 16);
    private final DataInputDeserializer in = new DataInputDeserializer();
    private int cursor;

    public BenchCase(SerializerVariant<T> variant, Profile profile, long seed) {
        this.variant = variant;
        this.serializer = variant.createSerializer(!variant.usesKryo());
        this.pool = poolSize(profile);
        this.objects = new Object[pool];
        this.bytes = new byte[pool][];
        OrderGenerator generator = new OrderGenerator(seed, profile);
        try {
            for (int i = 0; i < pool; i++) {
                T value = variant.map(generator.next());
                objects[i] = value;
                DataOutputSerializer tmp = new DataOutputSerializer(1024);
                serializer.serialize(value, tmp);
                bytes[i] = tmp.getCopyOfBuffer();
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static int poolSize(Profile profile) {
        switch (profile) {
            case SMALL:
                return 4096;
            case MEDIUM:
                return 1024;
            default:
                return 256;
        }
    }

    public int pool() {
        return pool;
    }

    public SerializerVariant<T> variant() {
        return variant;
    }

    public TypeSerializer<T> serializer() {
        return serializer;
    }

    public double averageSerializedBytes() {
        long total = 0;
        for (byte[] b : bytes) {
            total += b.length;
        }
        return (double) total / pool;
    }

    private int next() {
        return cursor++ & (pool - 1);
    }

    @SuppressWarnings("unchecked")
    public int serialize() throws IOException {
        out.clear();
        serializer.serialize((T) objects[next()], out);
        return out.length();
    }

    public T deserialize() throws IOException {
        in.setBuffer(bytes[next()]);
        return serializer.deserialize(in);
    }

    public T deserializeReuse() throws IOException {
        in.setBuffer(bytes[next()]);
        reuse = reuse == null ? serializer.deserialize(in) : serializer.deserialize(reuse, in);
        return reuse;
    }

    @SuppressWarnings("unchecked")
    public T roundTrip() throws IOException {
        out.clear();
        serializer.serialize((T) objects[next()], out);
        in.setBuffer(out.getSharedBuffer(), 0, out.length());
        return serializer.deserialize(in);
    }

    @SuppressWarnings("unchecked")
    public T copy() {
        return serializer.copy((T) objects[next()]);
    }
}
