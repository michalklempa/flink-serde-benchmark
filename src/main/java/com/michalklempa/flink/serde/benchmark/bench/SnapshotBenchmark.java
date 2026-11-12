package com.michalklempa.flink.serde.benchmark.bench;

import com.michalklempa.flink.serde.benchmark.serde.SerializerVariant;
import com.michalklempa.flink.serde.benchmark.serde.Variants;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class SnapshotBenchmark {

    @Param({"pojo-hinted", "pojo-plain", "pojo-kryo", "avro", "protobuf"})
    public String variant;

    private Runner runner;

    private interface Runner {
        Object snapshotAndCompat() throws IOException;
    }

    private static final class Typed<T> implements Runner {
        private final TypeSerializer<T> serializer;

        Typed(SerializerVariant<T> v) {
            this.serializer = v.createSerializer(!v.usesKryo());
        }

        @Override
        public Object snapshotAndCompat() throws IOException {
            DataOutputSerializer out = new DataOutputSerializer(256);
            TypeSerializerSnapshot.writeVersionedSnapshot(out, serializer.snapshotConfiguration());
            DataInputDeserializer in = new DataInputDeserializer(out.getSharedBuffer(), 0, out.length());
            TypeSerializerSnapshot<T> restored = TypeSerializerSnapshot.readVersionedSnapshot(
                    in, Thread.currentThread().getContextClassLoader());
            TypeSerializerSchemaCompatibility<T> compat = restored.resolveSchemaCompatibility(serializer);
            return compat;
        }
    }

    @Setup
    public void setup() {
        runner = new Typed<>(Variants.byName(variant));
    }

    @Benchmark
    public Object snapshotWriteReadAndCompat() throws IOException {
        return runner.snapshotAndCompat();
    }
}
