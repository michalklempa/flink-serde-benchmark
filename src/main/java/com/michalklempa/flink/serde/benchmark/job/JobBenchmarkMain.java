package com.michalklempa.flink.serde.benchmark.job;

import com.michalklempa.flink.serde.benchmark.bench.ReportBuilder;
import com.michalklempa.flink.serde.benchmark.data.Profile;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class JobBenchmarkMain {
    private JobBenchmarkMain() {
    }

    public static void main(String[] args) throws Exception {
        String mode = args.length > 0 ? args[0] : "quick";
        Path readme = Paths.get(args.length > 1 ? args[1] : "README.md");
        run(mode, readme);
    }

    static long records(String mode, Profile p) {
        if (mode.equals("smoke")) {
            return 3000;
        }
        long full = p == Profile.SMALL ? 2_000_000 : p == Profile.MEDIUM ? 500_000 : 50_000;
        return mode.equals("full") ? full : full / 10;
    }

    static long keys(String mode, Profile p) {
        if (mode.equals("smoke")) {
            return 50;
        }
        return p == Profile.SMALL ? 100_000 : p == Profile.MEDIUM ? 10_000 : 1_000;
    }

    public static void run(String mode, Path readme) throws IOException, InterruptedException {
        int repeats = mode.equals("full") ? 3 : 1;
        List<String> variants = mode.equals("smoke")
                ? Arrays.asList("protobuf", "pojo-hinted")
                : Arrays.asList("pojo-hinted", "pojo-plain", "pojo-kryo", "avro", "protobuf");
        List<Profile> profiles = mode.equals("smoke") ? Arrays.asList(Profile.SMALL) : Arrays.asList(Profile.values());
        List<JobReportBuilder.Row> rows = new ArrayList<>();
        int total = JobConfig.Kind.values().length * variants.size() * profiles.size() * repeats;
        int done = 0;
        for (JobConfig.Kind kind : JobConfig.Kind.values()) {
            for (Profile profile : profiles) {
                for (String variant : variants) {
                    List<Double> samples = new ArrayList<>();
                    for (int i = 0; i < repeats; i++) {
                        JobConfig config = new JobConfig(kind, variant, profile, records(mode, profile),
                                records(mode, profile) / 10, keys(mode, profile));
                        System.err.printf("[%d/%d] job %s %s %s%n", ++done, total, kind, variant, profile);
                        samples.add(runForked(config).recordsPerSecond());
                    }
                    Collections.sort(samples);
                    rows.add(new JobReportBuilder.Row(kind, variant, profile, samples.get(samples.size() / 2)));
                }
            }
        }
        String description = String.format(
                "Mode `%s`: parallelism 1, chaining off, one JVM per run, %d repeat(s), median. "
                        + "Java %s, %s, %d CPUs, run on %s.",
                mode, repeats, System.getProperty("java.version"), System.getProperty("os.name"),
                Runtime.getRuntime().availableProcessors(), LocalDate.now());
        String block = JobReportBuilder.build(rows, description);
        String content = new String(Files.readAllBytes(readme), StandardCharsets.UTF_8);
        Files.write(readme, ReportBuilder.replaceBlock(content, block, JobReportBuilder.START, JobReportBuilder.END)
                .getBytes(StandardCharsets.UTF_8));
    }

    private static JobResult runForked(JobConfig config) throws IOException, InterruptedException {
        List<String> cmd = new ArrayList<>();
        cmd.add(Paths.get(System.getProperty("java.home"), "bin", "java").toString());
        cmd.add("-cp");
        cmd.add(System.getProperty("java.class.path"));
        cmd.add(JobRunner.class.getName());
        cmd.addAll(Arrays.asList(config.toArgs()));
        Process process = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        String result = null;
        StringBuilder output = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append('\n');
                if (line.startsWith("RESULT ")) {
                    result = line;
                }
            }
        }
        if (process.waitFor() != 0 || result == null) {
            throw new IllegalStateException("job failed: " + String.join(" ", config.toArgs()) + "\n" + output);
        }
        return JobResult.fromLine(config, result);
    }
}
