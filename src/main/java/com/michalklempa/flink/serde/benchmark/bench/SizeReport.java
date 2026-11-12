package com.michalklempa.flink.serde.benchmark.bench;

import com.michalklempa.flink.serde.benchmark.data.Profile;
import com.michalklempa.flink.serde.benchmark.serde.SerializerVariant;
import com.michalklempa.flink.serde.benchmark.serde.Variants;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataOutputSerializer;

public final class SizeReport {
    private SizeReport() {
    }

    public static final class Row {
        public final String variant;
        public final Profile profile;
        public final double serializedBytes;
        public final int snapshotBytes;

        Row(String variant, Profile profile, double serializedBytes, int snapshotBytes) {
            this.variant = variant;
            this.profile = profile;
            this.serializedBytes = serializedBytes;
            this.snapshotBytes = snapshotBytes;
        }
    }

    public static List<Row> collect() throws IOException {
        List<Row> rows = new ArrayList<>();
        for (SerializerVariant<?> variant : Variants.all()) {
            for (Profile profile : Profile.values()) {
                rows.add(row(variant, profile));
            }
        }
        return rows;
    }

    private static <T> Row row(SerializerVariant<T> variant, Profile profile) throws IOException {
        BenchCase<T> benchCase = new BenchCase<>(variant, profile, 12345L);
        TypeSerializer<T> serializer = benchCase.serializer();
        DataOutputSerializer out = new DataOutputSerializer(256);
        TypeSerializerSnapshot.writeVersionedSnapshot(out, serializer.snapshotConfiguration());
        return new Row(variant.name(), profile, benchCase.averageSerializedBytes(), out.length());
    }

    public static void main(String[] args) throws IOException {
        System.out.printf("%-12s %-7s %15s %15s%n", "variant", "profile", "serialized B", "snapshot B");
        for (Row r : collect()) {
            System.out.printf("%-12s %-7s %15.1f %15d%n", r.variant, r.profile, r.serializedBytes, r.snapshotBytes);
        }
    }
}
