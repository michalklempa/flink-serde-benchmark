package com.michalklempa.flink.serde.benchmark.bench;

import com.michalklempa.flink.serde.benchmark.data.Profile;
import com.michalklempa.flink.serde.benchmark.serde.Variants;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
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
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 5, jvmArgs = {"-Xms2g", "-Xmx2g", "-XX:+UseParallelGC"})
public class SerdeBenchmark {

    @Param({"pojo-hinted", "pojo-plain", "pojo-kryo", "avro", "protobuf"})
    public String variant;

    @Param({"SMALL", "MEDIUM", "LARGE"})
    public String profile;

    private BenchCase<?> benchCase;

    @Setup
    public void setup() {
        benchCase = new BenchCase<>(Variants.byName(variant), Profile.valueOf(profile), 12345L);
    }

    @Benchmark
    public int serialize() throws IOException {
        return benchCase.serialize();
    }

    @Benchmark
    public Object deserialize() throws IOException {
        return benchCase.deserialize();
    }

    @Benchmark
    public Object deserializeReuse() throws IOException {
        return benchCase.deserializeReuse();
    }

    @Benchmark
    public int roundTrip() throws IOException {
        return benchCase.roundTrip();
    }

    @Benchmark
    public Object copy() {
        return benchCase.copy();
    }
}
