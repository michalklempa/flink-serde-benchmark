package com.michalklempa.flink.serde.benchmark.bench;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

public final class ReportMain {
    private ReportMain() {
    }

    public static void main(String[] args) throws Exception {
        String mode = args.length > 0 ? args[0] : "quick";
        Path readme = Paths.get(args.length > 1 ? args[1] : "README.md");
        run(mode, readme);
    }

    public static void run(String mode, Path readme) throws RunnerException, IOException {
        ChainedOptionsBuilder b = new OptionsBuilder()
                .include(SerdeBenchmark.class.getSimpleName())
                .include(SnapshotBenchmark.class.getSimpleName())
                .addProfiler(GCProfiler.class)
                .shouldFailOnError(true);
        String description;
        switch (mode) {
            case "full":
                description = "Mode `full`: 5 forks, 5 x 1 s warmup, 5 x 1 s measurement.";
                break;
            case "quick":
                b.forks(1).warmupIterations(1).warmupTime(TimeValue.seconds(1))
                        .measurementIterations(2).measurementTime(TimeValue.seconds(1));
                description = "Mode `quick`: 1 fork, 1 x 1 s warmup, 2 x 1 s measurement. Scores are noisy.";
                break;
            case "smoke":
                b = new OptionsBuilder()
                        .include(SerdeBenchmark.class.getSimpleName() + ".roundTrip")
                        .include(SnapshotBenchmark.class.getSimpleName())
                        .param("variant", "protobuf", "pojo-hinted")
                        .param("profile", "SMALL")
                        .addProfiler(GCProfiler.class)
                        .forks(0).warmupIterations(0).warmupTime(TimeValue.milliseconds(50))
                        .measurementIterations(1).measurementTime(TimeValue.milliseconds(100))
                        .shouldFailOnError(true);
                description = "Mode `smoke`: wiring check only, numbers are meaningless.";
                break;
            default:
                throw new IllegalArgumentException("mode must be quick, full or smoke");
        }
        Collection<RunResult> results = new Runner(b.build()).run();
        description += String.format("%nJava %s, %s, %d CPUs, run on %s.", System.getProperty("java.version"),
                System.getProperty("os.name"), Runtime.getRuntime().availableProcessors(), LocalDate.now());
        String block = ReportBuilder.build(toMeasurements(results), SizeReport.collect(), description);
        String content = new String(Files.readAllBytes(readme), StandardCharsets.UTF_8);
        Files.write(readme, ReportBuilder.replaceBlock(content, block).getBytes(StandardCharsets.UTF_8));
    }

    static List<ReportBuilder.Measurement> toMeasurements(Collection<RunResult> results) {
        List<ReportBuilder.Measurement> list = new ArrayList<>();
        for (RunResult r : results) {
            String benchmark = r.getParams().getBenchmark();
            String method = benchmark.substring(benchmark.lastIndexOf('.') + 1);
            double alloc = Double.NaN;
            for (Map.Entry<String, Result> e : r.getSecondaryResults().entrySet()) {
                if (e.getKey().endsWith("gc.alloc.rate.norm")) {
                    alloc = e.getValue().getScore();
                }
            }
            list.add(new ReportBuilder.Measurement(method, r.getParams().getParam("variant"),
                    r.getParams().getParam("profile"), r.getPrimaryResult().getScore(),
                    r.getPrimaryResult().getScoreError(), alloc));
        }
        return list;
    }
}
