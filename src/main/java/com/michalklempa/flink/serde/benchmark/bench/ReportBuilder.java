package com.michalklempa.flink.serde.benchmark.bench;

import com.michalklempa.flink.serde.benchmark.data.Profile;
import com.michalklempa.flink.serde.benchmark.serde.Variants;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;

public final class ReportBuilder {
    public static final String START = "<!-- results:start -->";
    public static final String END = "<!-- results:end -->";
    private static final int BAR_WIDTH = 30;

    private ReportBuilder() {
    }

    public static final class Measurement {
        public final String method;
        public final String variant;
        public final String profile;
        public final double score;
        public final double error;
        public final double allocBytesPerOp;

        public Measurement(String method, String variant, String profile, double score, double error,
                           double allocBytesPerOp) {
            this.method = method;
            this.variant = variant;
            this.profile = profile;
            this.score = score;
            this.error = error;
            this.allocBytesPerOp = allocBytesPerOp;
        }
    }

    public static String replaceBlock(String readme, String block) {
        return replaceBlock(readme, block, START, END);
    }

    public static String replaceBlock(String readme, String block, String start, String end) {
        int s = readme.indexOf(start);
        int e = readme.indexOf(end);
        if (s < 0 || e < s) {
            throw new IllegalStateException("README is missing " + start + " / " + end + " markers");
        }
        return readme.substring(0, s + start.length()) + "\n" + block + "\n" + readme.substring(e);
    }

    public static String build(List<Measurement> ms, List<SizeReport.Row> sizes, String runDescription) {
        StringBuilder sb = new StringBuilder();
        sb.append(runDescription).append("\n\n");
        String[][] metrics = {
                {"serialize", "serialize"}, {"deserialize", "deserialize (new object each time)"}, {"deserializeReuse", "deserialize (reusing one object)"},
                {"roundTrip", "round trip (serialize + deserialize)"}, {"copy", "copy"}};
        for (String[] m : metrics) {
            if (profiles(ms, m[0]).isEmpty()) {
                continue;
            }
            sb.append("#### ").append(m[1]).append(" (ns/op, lower is better)\n\n");
            sb.append(profileTable(ms, m[0], false));
            sb.append(bars(ms, m[0]));
        }
        if (!profiles(ms, "roundTrip").isEmpty()) {
            sb.append("#### allocation per round trip (B/op, lower is better)\n\n");
            sb.append(profileTable(ms, "roundTrip", true));
        }
        sb.append("#### serialized size (bytes per order, lower is better)\n\n");
        sb.append(sizeTable(sizes, false));
        sb.append("#### serializer snapshot (written into every checkpoint)\n\n");
        sb.append(snapshotTable(ms, sizes));
        return sb.toString();
    }

    private static List<String> variantOrder(List<Measurement> ms, String method) {
        List<String> present = new ArrayList<>();
        for (String v : Variants.all().stream().map(x -> x.name()).collect(Collectors.toList())) {
            if (ms.stream().anyMatch(x -> x.method.equals(method) && v.equals(x.variant))) {
                present.add(v);
            }
        }
        present.sort(Comparator.comparingDouble(v -> sortKey(ms, method, v)));
        return present;
    }

    private static double sortKey(List<Measurement> ms, String method, String variant) {
        return ms.stream()
                .filter(x -> x.method.equals(method) && variant.equals(x.variant))
                .mapToDouble(x -> x.score)
                .max()
                .orElse(Double.MAX_VALUE);
    }

    private static List<Profile> profiles(List<Measurement> ms, String method) {
        List<Profile> result = new ArrayList<>();
        for (Profile p : Profile.values()) {
            if (ms.stream().anyMatch(x -> x.method.equals(method) && p.name().equals(x.profile))) {
                result.add(p);
            }
        }
        return result;
    }

    private static Optional<Measurement> find(List<Measurement> ms, String method, String variant, Profile p) {
        return ms.stream()
                .filter(x -> x.method.equals(method) && variant.equals(x.variant) && p.name().equals(x.profile))
                .findFirst();
    }

    private static String profileTable(List<Measurement> ms, String method, boolean alloc) {
        List<Profile> ps = profiles(ms, method);
        StringBuilder sb = new StringBuilder("| variant |");
        StringBuilder sep = new StringBuilder("|---|");
        for (Profile p : ps) {
            sb.append(' ').append(p).append(" |");
            sep.append("---:|");
        }
        sb.append('\n').append(sep).append('\n');
        for (String v : variantOrder(ms, method)) {
            sb.append("| ").append(v).append(" |");
            for (Profile p : ps) {
                String cell = find(ms, method, v, p)
                        .map(x -> alloc ? number(x.allocBytesPerOp) : withError(x))
                        .orElse("-");
                sb.append(' ').append(cell).append(" |");
            }
            sb.append('\n');
        }
        return sb.append('\n').toString();
    }

    private static String bars(List<Measurement> ms, String method) {
        StringBuilder sb = new StringBuilder("```\n");
        for (Profile p : profiles(ms, method)) {
            List<String> vs = variantOrder(ms, method);
            double max = vs.stream()
                    .map(v -> find(ms, method, v, p))
                    .filter(Optional::isPresent)
                    .mapToDouble(o -> o.get().score)
                    .max()
                    .orElse(1);
            sb.append(method).append(' ').append(p).append(" (ns/op)\n");
            for (String v : vs) {
                Optional<Measurement> m = find(ms, method, v, p);
                if (!m.isPresent()) {
                    continue;
                }
                int filled = (int) Math.max(1, Math.round(m.get().score / max * BAR_WIDTH));
                sb.append(String.format(Locale.ROOT, "%-12s ", v));
                for (int i = 0; i < BAR_WIDTH; i++) {
                    sb.append(i < filled ? '█' : '░');
                }
                sb.append(String.format(Locale.ROOT, " %12s%n", number(m.get().score)));
            }
            sb.append('\n');
        }
        return sb.toString().replaceAll("\n+$", "\n") + "```\n\n";
    }

    private static String sizeTable(List<SizeReport.Row> sizes, boolean snapshot) {
        StringBuilder sb = new StringBuilder("| variant |");
        StringBuilder sep = new StringBuilder("|---|");
        for (Profile p : Profile.values()) {
            sb.append(' ').append(p).append(" |");
            sep.append("---:|");
        }
        sb.append('\n').append(sep).append('\n');
        for (String v : Variants.all().stream().map(x -> x.name()).collect(Collectors.toList())) {
            sb.append("| ").append(v).append(" |");
            for (Profile p : Profile.values()) {
                String cell = sizes.stream()
                        .filter(r -> r.variant.equals(v) && r.profile == p)
                        .findFirst()
                        .map(r -> number(snapshot ? r.snapshotBytes : r.serializedBytes))
                        .orElse("-");
                sb.append(' ').append(cell).append(" |");
            }
            sb.append('\n');
        }
        return sb.append('\n').toString();
    }

    private static String snapshotTable(List<Measurement> ms, List<SizeReport.Row> sizes) {
        StringBuilder sb = new StringBuilder("| variant | snapshot bytes | write + read + compatibility check (µs/op) |\n|---|---:|---:|\n");
        for (String v : Variants.all().stream().map(x -> x.name()).collect(Collectors.toList())) {
            String bytes = sizes.stream().filter(r -> r.variant.equals(v)).findFirst()
                    .map(r -> number(r.snapshotBytes)).orElse("-");
            String time = ms.stream().filter(x -> x.method.equals("snapshotWriteReadAndCompat") && v.equals(x.variant))
                    .findFirst().map(ReportBuilder::withError).orElse("-");
            sb.append("| ").append(v).append(" | ").append(bytes).append(" | ").append(time).append(" |\n");
        }
        return sb.append('\n').toString();
    }

    private static String withError(Measurement x) {
        return Double.isNaN(x.error) ? number(x.score) : number(x.score) + " ± " + number(x.error);
    }

    static String number(double v) {
        if (Double.isNaN(v)) {
            return "-";
        }
        return v < 100 ? String.format(Locale.ROOT, "%.1f", v) : String.format(Locale.ROOT, "%,.0f", v);
    }
}
