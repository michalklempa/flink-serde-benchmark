package com.michalklempa.flink.serde.benchmark.bench;

import com.michalklempa.flink.serde.benchmark.data.OrderGenerator;
import com.michalklempa.flink.serde.benchmark.data.Profile;
import com.michalklempa.flink.serde.benchmark.serde.SerializerVariant;
import java.io.IOException;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

public final class BenchCase<T> {
    public static final int POOL = 64;

    private final SerializerVariant<T> variant;
    private final TypeSerializer<T> serializer;
    private final Object[] objects = new Object[POOL];
    private final byte[][] bytes = new byte[POOL][];
    private final DataOutputSerializer out = new DataOutputSerializer(1 << 16);
    private final DataInputDeserializer in = new DataInputDeserializer();
    private int cursor;

    public BenchCase(SerializerVariant<T> variant, Profile profile, long seed) {
        this.variant = variant;
        this.serializer = variant.createSerializer(!variant.usesKryo());
        OrderGenerator generator = new OrderGenerator(seed, profile);
        try {
            for (int i = 0; i < POOL; i++) {
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
        return (double) total / POOL;
    }

    private int next() {
        return cursor++ & (POOL - 1);
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
