package com.michalklempa.flink.serde.benchmark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.michalklempa.flink.serde.benchmark.bench.BenchCase;
import com.michalklempa.flink.serde.benchmark.bench.SerdeBenchmark;
import com.michalklempa.flink.serde.benchmark.bench.SizeReport;
import com.michalklempa.flink.serde.benchmark.bench.SnapshotBenchmark;
import com.michalklempa.flink.serde.benchmark.data.Profile;
import com.michalklempa.flink.serde.benchmark.serde.SerializerVariant;
import com.michalklempa.flink.serde.benchmark.serde.Variants;
import java.io.IOException;
import java.util.Collection;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

class BenchmarkSmokeTest {

    static Stream<SerializerVariant<?>> variants() {
        return Variants.all().stream();
    }

    @ParameterizedTest
    @MethodSource("variants")
    void benchCaseOperationsRun(SerializerVariant<?> variant) throws IOException {
        runOperations(variant);
    }

    private static <T> void runOperations(SerializerVariant<T> variant) throws IOException {
        BenchCase<T> c = new BenchCase<>(variant, Profile.SMALL, 1L);
        for (int i = 0; i < BenchCase.POOL + 3; i++) {
            assertTrue(c.serialize() > 0);
            assertNotNull(c.deserialize());
            assertNotNull(c.roundTrip());
            assertNotNull(c.copy());
        }
        assertTrue(c.averageSerializedBytes() > 0);
    }

    @Test
    void sizeReportCoversAllVariantsAndProfiles() throws IOException {
        assertEquals(Variants.all().size() * Profile.values().length, SizeReport.collect().size());
        SizeReport.collect().forEach(r -> assertTrue(r.snapshotBytes > 0 && r.serializedBytes > 0, r.variant));
    }

    @Test
    void jmhRunsEmbedded() throws Exception {
        Options options = new OptionsBuilder()
                .include(SerdeBenchmark.class.getSimpleName() + ".roundTrip")
                .include(SnapshotBenchmark.class.getSimpleName())
                .param("variant", "protobuf", "pojo-hinted")
                .param("profile", "SMALL")
                .forks(0)
                .warmupIterations(0)
                .measurementIterations(1)
                .warmupTime(TimeValue.milliseconds(50))
                .measurementTime(TimeValue.milliseconds(100))
                .shouldFailOnError(true)
                .build();
        Collection<RunResult> results = new Runner(options).run();
        assertFalse(results.isEmpty());
    }

    @Test
    void reportMainRewritesReadmeBlock(@org.junit.jupiter.api.io.TempDir java.nio.file.Path dir) throws Exception {
        java.nio.file.Path readme = dir.resolve("README.md");
        java.nio.file.Files.write(readme, ("# t\n" + com.michalklempa.flink.serde.benchmark.bench.ReportBuilder.START
                + "\nold\n" + com.michalklempa.flink.serde.benchmark.bench.ReportBuilder.END + "\ntail\n")
                .getBytes(java.nio.charset.StandardCharsets.UTF_8));
        com.michalklempa.flink.serde.benchmark.bench.ReportMain.run("smoke", readme);
        String content = new String(java.nio.file.Files.readAllBytes(readme), java.nio.charset.StandardCharsets.UTF_8);
        assertTrue(content.contains("#### round trip"));
        assertTrue(content.contains("█"));
        assertTrue(content.contains("| protobuf |"));
        assertTrue(content.startsWith("# t\n") && content.endsWith("tail\n"));
        assertFalse(content.contains("\nold\n"));
    }
}
