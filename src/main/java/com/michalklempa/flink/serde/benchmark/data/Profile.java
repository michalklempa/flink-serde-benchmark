package com.michalklempa.flink.serde.benchmark.data;

public enum Profile {
    SMALL(1, 1, 1, 16, 6),
    MEDIUM(18, 8, 12, 256, 12),
    LARGE(500, 40, 100, 8192, 16);

    public final int items;
    public final int tags;
    public final int attributes;
    public final int payloadBytes;
    public final int stringLength;

    Profile(int items, int tags, int attributes, int payloadBytes, int stringLength) {
        this.items = items;
        this.tags = tags;
        this.attributes = attributes;
        this.payloadBytes = payloadBytes;
        this.stringLength = stringLength;
    }
}
