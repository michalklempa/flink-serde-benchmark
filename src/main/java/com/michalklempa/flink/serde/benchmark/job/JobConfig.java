package com.michalklempa.flink.serde.benchmark.job;

import com.michalklempa.flink.serde.benchmark.data.Profile;
import java.io.Serializable;

public final class JobConfig implements Serializable {
    public enum Kind {
        A, B
    }

    public final Kind kind;
    public final String variant;
    public final Profile profile;
    public final long records;
    public final long warmup;
    public final long keys;

    public JobConfig(Kind kind, String variant, Profile profile, long records, long warmup, long keys) {
        this.kind = kind;
        this.variant = variant;
        this.profile = profile;
        this.records = records;
        this.warmup = warmup;
        this.keys = keys;
    }

    public static JobConfig parse(String[] args) {
        Kind kind = null;
        String variant = null;
        Profile profile = null;
        long records = 0;
        long warmup = -1;
        long keys = 0;
        for (String a : args) {
            String[] kv = a.split("=", 2);
            switch (kv[0]) {
                case "--job":
                    kind = Kind.valueOf(kv[1]);
                    break;
                case "--variant":
                    variant = kv[1];
                    break;
                case "--profile":
                    profile = Profile.valueOf(kv[1]);
                    break;
                case "--records":
                    records = Long.parseLong(kv[1]);
                    break;
                case "--warmup":
                    warmup = Long.parseLong(kv[1]);
                    break;
                case "--keys":
                    keys = Long.parseLong(kv[1]);
                    break;
                default:
                    throw new IllegalArgumentException("unknown argument " + a);
            }
        }
        if (kind == null || variant == null || profile == null || records <= 0 || keys <= 0) {
            throw new IllegalArgumentException(
                    "required: --job=A|B --variant=<name> --profile=<P> --records=<n> --keys=<k> [--warmup=<w>]");
        }
        return new JobConfig(kind, variant, profile, records, warmup >= 0 ? warmup : records / 10, keys);
    }

    public String[] toArgs() {
        return new String[] {"--job=" + kind, "--variant=" + variant, "--profile=" + profile,
                "--records=" + records, "--warmup=" + warmup, "--keys=" + keys};
    }
}
