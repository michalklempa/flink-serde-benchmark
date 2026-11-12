package com.michalklempa.flink.serde.benchmark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.michalklempa.flink.serde.benchmark.data.OrderGenerator;
import com.michalklempa.flink.serde.benchmark.data.OrderSpec;
import com.michalklempa.flink.serde.benchmark.data.Profile;
import com.michalklempa.flink.serde.benchmark.serde.SerializerVariant;
import com.michalklempa.flink.serde.benchmark.serde.Variants;
import java.io.IOException;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SerializerVariantsTest {

    static Stream<SerializerVariant<?>> variants() {
        return Variants.all().stream();
    }

    static Stream<Arguments> variantsAndProfiles() {
        return Variants.all().stream()
                .flatMap(v -> Stream.of(Profile.values()).map(p -> Arguments.of(v, p)));
    }

    @ParameterizedTest
    @MethodSource("variants")
    void createsExpectedSerializer(SerializerVariant<?> variant) {
        assertInstanceOf(variant.expectedSerializer(), variant.createSerializer(false));
    }

    @ParameterizedTest
    @MethodSource("variants")
    void genericTypesDisabledOnlyBreaksKryoVariants(SerializerVariant<?> variant) {
        if (variant.usesKryo()) {
            assertThrows(UnsupportedOperationException.class, () -> variant.createSerializer(true));
        } else {
            assertInstanceOf(variant.expectedSerializer(), variant.createSerializer(true));
        }
    }

    @ParameterizedTest
    @MethodSource("variantsAndProfiles")
    void roundTripPreservesContent(SerializerVariant<?> variant, Profile profile) throws IOException {
        roundTrip(variant, profile);
    }

    @ParameterizedTest
    @MethodSource("variantsAndProfiles")
    void copyPreservesContent(SerializerVariant<?> variant, Profile profile) {
        copy(variant, profile);
    }

    private static <T> void roundTrip(SerializerVariant<T> variant, Profile profile) throws IOException {
        TypeSerializer<T> serializer = variant.createSerializer(false);
        OrderGenerator generator = new OrderGenerator(11, profile);
        for (int i = 0; i < 3; i++) {
            OrderSpec spec = generator.next();
            DataOutputSerializer out = new DataOutputSerializer(256);
            serializer.serialize(variant.map(spec), out);
            T back = serializer.deserialize(new DataInputDeserializer(out.getCopyOfBuffer()));
            assertEquals(Canonical.of(spec), Canonical.of(back), variant.name());
        }
    }

    private static <T> void copy(SerializerVariant<T> variant, Profile profile) {
        TypeSerializer<T> serializer = variant.createSerializer(false);
        OrderSpec spec = new OrderGenerator(13, profile).next();
        T copy = serializer.copy(variant.map(spec));
        assertEquals(Canonical.of(spec), Canonical.of(copy), variant.name());
    }
}
