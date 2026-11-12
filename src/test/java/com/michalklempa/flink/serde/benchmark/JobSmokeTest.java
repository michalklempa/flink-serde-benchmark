package com.michalklempa.flink.serde.benchmark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.michalklempa.flink.serde.benchmark.data.Profile;
import com.michalklempa.flink.serde.benchmark.job.JobConfig;
import com.michalklempa.flink.serde.benchmark.job.JobResult;
import com.michalklempa.flink.serde.benchmark.job.JobRunner;
import com.michalklempa.flink.serde.benchmark.serde.SerializerVariant;
import com.michalklempa.flink.serde.benchmark.serde.Variants;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class JobSmokeTest {

    static Stream<Arguments> jobs() {
        return Variants.all().stream()
                .flatMap(v -> Stream.of(JobConfig.Kind.values()).map(k -> Arguments.of(v, k)));
    }

    @ParameterizedTest
    @MethodSource("jobs")
    void jobProcessesAllRecords(SerializerVariant<?> variant, JobConfig.Kind kind) throws Exception {
        JobConfig config = new JobConfig(kind, variant.name(), Profile.SMALL, 3000, 300, 50);
        JobResult result = JobRunner.run(config);
        assertEquals(2700, result.measuredRecords);
        assertTrue(result.elapsedNanos > 0, variant.name() + " " + kind);
        assertTrue(result.recordsPerSecond() > 0);
    }

    @ParameterizedTest
    @MethodSource("jobs")
    void configRoundTripsThroughArguments(SerializerVariant<?> variant, JobConfig.Kind kind) {
        JobConfig config = new JobConfig(kind, variant.name(), Profile.MEDIUM, 10, 1, 5);
        JobConfig parsed = JobConfig.parse(config.toArgs());
        assertEquals(config.variant, parsed.variant);
        assertEquals(config.kind, parsed.kind);
        assertEquals(config.keys, parsed.keys);
        assertEquals(config.warmup, parsed.warmup);
    }

    @org.junit.jupiter.api.Test
    void driverRewritesJobsBlock(@org.junit.jupiter.api.io.TempDir java.nio.file.Path dir) throws Exception {
        java.nio.file.Path readme = dir.resolve("README.md");
        java.nio.file.Files.write(readme, ("head\n" + com.michalklempa.flink.serde.benchmark.job.JobReportBuilder.START
                + "\nold\n" + com.michalklempa.flink.serde.benchmark.job.JobReportBuilder.END + "\ntail\n")
                .getBytes(java.nio.charset.StandardCharsets.UTF_8));
        com.michalklempa.flink.serde.benchmark.job.JobBenchmarkMain.run("smoke", readme);
        String content = new String(java.nio.file.Files.readAllBytes(readme), java.nio.charset.StandardCharsets.UTF_8);
        assertTrue(content.contains("#### Job A"));
        assertTrue(content.contains("#### Job B"));
        assertTrue(content.contains("| protobuf |"));
        assertTrue(content.contains("█"));
        assertTrue(content.startsWith("head\n") && content.endsWith("tail\n"));
    }
}
