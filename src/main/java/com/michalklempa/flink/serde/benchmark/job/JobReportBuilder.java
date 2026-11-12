package com.michalklempa.flink.serde.benchmark.job;

import com.michalklempa.flink.serde.benchmark.data.Profile;
import com.michalklempa.flink.serde.benchmark.serde.Variants;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;

public final class JobReportBuilder {
    public static final String START = "<!-- jobs:start -->";
    public static final String END = "<!-- jobs:end -->";
    private static final int BAR_WIDTH = 30;

    private JobReportBuilder() {
    }

    public static final class Row {
        public final JobConfig.Kind kind;
        public final String variant;
        public final Profile profile;
        public final double recordsPerSecond;

        public Row(JobConfig.Kind kind, String variant, Profile profile, double recordsPerSecond) {
            this.kind = kind;
            this.variant = variant;
            this.profile = profile;
            this.recordsPerSecond = recordsPerSecond;
        }
    }

    public static String build(List<Row> rows, String description) {
        StringBuilder sb = new StringBuilder(description).append("\n\n");
        sb.append("#### Job A: operator to operator (records/s, higher is better)\n\n");
        sb.append(table(rows, JobConfig.Kind.A));
        sb.append(bars(rows, JobConfig.Kind.A));
        sb.append("#### Job B: keyed ValueState on RocksDB (records/s, higher is better)\n\n");
        sb.append(table(rows, JobConfig.Kind.B));
        sb.append(bars(rows, JobConfig.Kind.B));
        sb.append("#### State cost per record (µs, Job B minus Job A, lower is better)\n\n");
        sb.append(costTable(rows));
        return sb.toString();
    }

    private static List<String> variants(List<Row> rows, JobConfig.Kind kind) {
        List<String> present = Variants.all().stream().map(v -> v.name())
                .filter(v -> rows.stream().anyMatch(r -> r.kind == kind && r.variant.equals(v)))
                .collect(Collectors.toList());
        present.sort(Comparator.comparingDouble((String v) -> -rows.stream()
                .filter(r -> r.kind == kind && r.variant.equals(v))
                .mapToDouble(r -> r.recordsPerSecond).max().orElse(0)));
        return present;
    }

    private static List<Profile> profiles(List<Row> rows, JobConfig.Kind kind) {
        List<Profile> result = new ArrayList<>();
        for (Profile p : Profile.values()) {
            if (rows.stream().anyMatch(r -> r.kind == kind && r.profile == p)) {
                result.add(p);
            }
        }
        return result;
    }

    private static Optional<Row> find(List<Row> rows, JobConfig.Kind kind, String variant, Profile p) {
        return rows.stream().filter(r -> r.kind == kind && r.variant.equals(variant) && r.profile == p).findFirst();
    }

    private static String table(List<Row> rows, JobConfig.Kind kind) {
        List<Profile> ps = profiles(rows, kind);
        StringBuilder sb = new StringBuilder("| variant |");
        StringBuilder sep = new StringBuilder("|---|");
        for (Profile p : ps) {
            sb.append(' ').append(p).append(" |");
            sep.append("---:|");
        }
        sb.append('\n').append(sep).append('\n');
        for (String v : variants(rows, kind)) {
            sb.append("| ").append(v).append(" |");
            for (Profile p : ps) {
                sb.append(' ').append(find(rows, kind, v, p).map(r -> number(r.recordsPerSecond)).orElse("-"))
                        .append(" |");
            }
            sb.append('\n');
        }
        return sb.append('\n').toString();
    }

    private static String bars(List<Row> rows, JobConfig.Kind kind) {
        StringBuilder sb = new StringBuilder("```\n");
        for (Profile p : profiles(rows, kind)) {
            double max = rows.stream().filter(r -> r.kind == kind && r.profile == p)
                    .mapToDouble(r -> r.recordsPerSecond).max().orElse(1);
            sb.append("job ").append(kind).append(' ').append(p).append(" (records/s)\n");
            for (String v : variants(rows, kind)) {
                Optional<Row> r = find(rows, kind, v, p);
                if (!r.isPresent()) {
                    continue;
                }
                int filled = (int) Math.max(1, Math.round(r.get().recordsPerSecond / max * BAR_WIDTH));
                sb.append(String.format(Locale.ROOT, "%-12s ", v));
                for (int i = 0; i < BAR_WIDTH; i++) {
                    sb.append(i < filled ? '█' : '░');
                }
                sb.append(String.format(Locale.ROOT, " %12s%n", number(r.get().recordsPerSecond)));
            }
            sb.append('\n');
        }
        return sb.toString().replaceAll("\n+$", "\n") + "```\n\n";
    }

    private static String costTable(List<Row> rows) {
        List<Profile> ps = profiles(rows, JobConfig.Kind.B);
        StringBuilder sb = new StringBuilder("| variant |");
        StringBuilder sep = new StringBuilder("|---|");
        for (Profile p : ps) {
            sb.append(' ').append(p).append(" |");
            sep.append("---:|");
        }
        sb.append('\n').append(sep).append('\n');
        for (String v : variants(rows, JobConfig.Kind.B)) {
            sb.append("| ").append(v).append(" |");
            for (Profile p : ps) {
                Optional<Row> a = find(rows, JobConfig.Kind.A, v, p);
                Optional<Row> b = find(rows, JobConfig.Kind.B, v, p);
                String cell = a.isPresent() && b.isPresent()
                        ? number(1e6 / b.get().recordsPerSecond - 1e6 / a.get().recordsPerSecond) : "-";
                sb.append(' ').append(cell).append(" |");
            }
            sb.append('\n');
        }
        return sb.append('\n').toString();
    }

    static String number(double v) {
        return v < 100 ? String.format(Locale.ROOT, "%.1f", v) : String.format(Locale.ROOT, "%,.0f", v);
    }
}
