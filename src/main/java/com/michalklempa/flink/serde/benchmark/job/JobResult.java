package com.michalklempa.flink.serde.benchmark.job;

public final class JobResult {
    public final JobConfig config;
    public final long measuredRecords;
    public final long elapsedNanos;

    public JobResult(JobConfig config, long measuredRecords, long elapsedNanos) {
        this.config = config;
        this.measuredRecords = measuredRecords;
        this.elapsedNanos = elapsedNanos;
    }

    public double recordsPerSecond() {
        return measuredRecords * 1e9 / elapsedNanos;
    }

    public String toLine() {
        return String.format("RESULT job=%s variant=%s profile=%s measured=%d elapsedNanos=%d",
                config.kind, config.variant, config.profile, measuredRecords, elapsedNanos);
    }

    public static JobResult fromLine(JobConfig config, String line) {
        long measured = 0;
        long elapsed = 0;
        for (String part : line.split(" ")) {
            if (part.startsWith("measured=")) {
                measured = Long.parseLong(part.substring(9));
            } else if (part.startsWith("elapsedNanos=")) {
                elapsed = Long.parseLong(part.substring(13));
            }
        }
        return new JobResult(config, measured, elapsed);
    }
}
