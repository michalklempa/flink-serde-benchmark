package com.michalklempa.flink.serde.benchmark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.michalklempa.flink.serde.benchmark.data.Mappers;
import com.michalklempa.flink.serde.benchmark.data.OrderGenerator;
import com.michalklempa.flink.serde.benchmark.data.OrderSpec;
import com.michalklempa.flink.serde.benchmark.data.Profile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class OrderGeneratorTest {

    @ParameterizedTest
    @EnumSource(Profile.class)
    void sameSeedGivesSameData(Profile profile) {
        OrderGenerator a = new OrderGenerator(42, profile);
        OrderGenerator b = new OrderGenerator(42, profile);
        for (int i = 0; i < 5; i++) {
            assertEquals(Canonical.of(a.next()), Canonical.of(b.next()));
        }
    }

    @ParameterizedTest
    @EnumSource(Profile.class)
    void allRepresentationsHoldSameContent(Profile profile) {
        OrderGenerator generator = new OrderGenerator(7, profile);
        for (int i = 0; i < 5; i++) {
            OrderSpec spec = generator.next();
            String expected = Canonical.of(spec);
            assertEquals(expected, Canonical.of(Mappers.toPojo(spec)));
            assertEquals(expected, Canonical.of(Mappers.toPojoPlain(spec)));
            assertEquals(expected, Canonical.of(Mappers.toAvro(spec)));
            assertEquals(expected, Canonical.of(Mappers.toProto(spec)));
        }
    }

    @Test
    void profilesHaveExpectedSizes() {
        int small = Mappers.toProto(new OrderGenerator(1, Profile.SMALL).next()).getSerializedSize();
        int medium = Mappers.toProto(new OrderGenerator(1, Profile.MEDIUM).next()).getSerializedSize();
        int large = Mappers.toProto(new OrderGenerator(1, Profile.LARGE).next()).getSerializedSize();
        assertTrue(small >= 80 && small <= 400, "small=" + small);
        assertTrue(medium >= 1500 && medium <= 3000, "medium=" + medium);
        assertTrue(large >= 40_000 && large <= 70_000, "large=" + large);
    }
}
