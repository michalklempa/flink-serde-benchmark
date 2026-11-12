package com.michalklempa.flink.serde.benchmark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.michalklempa.flink.serde.benchmark.data.Mappers;
import com.michalklempa.flink.serde.benchmark.data.OrderGenerator;
import com.michalklempa.flink.serde.benchmark.data.OrderSpec;
import com.michalklempa.flink.serde.benchmark.data.Profile;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
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
            assertEquals(canonical(a.next()), canonical(b.next()));
        }
    }

    @ParameterizedTest
    @EnumSource(Profile.class)
    void allRepresentationsHoldSameContent(Profile profile) {
        OrderGenerator generator = new OrderGenerator(7, profile);
        for (int i = 0; i < 5; i++) {
            OrderSpec spec = generator.next();
            String expected = canonical(spec);
            assertEquals(expected, canonical(Mappers.toPojo(spec)));
            assertEquals(expected, canonical(Mappers.toPojoPlain(spec)));
            assertEquals(expected, canonical(Mappers.toAvro(spec)));
            assertEquals(expected, canonical(Mappers.toProto(spec)));
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

    private static String line(Object... parts) {
        return Arrays.stream(parts).map(String::valueOf).collect(Collectors.joining("|")) + "\n";
    }

    private static String hex(byte[] b) {
        return Arrays.toString(b);
    }

    private static String map(Map<String, String> m) {
        return new TreeMap<>(m).toString();
    }

    private static String canonical(OrderSpec s) {
        StringBuilder sb = new StringBuilder();
        sb.append(line(s.orderId, s.createdAtMillis, s.updatedAtMillis, s.status, s.totalAmount, hex(s.payload)));
        sb.append(line(s.customer.customerId, s.customer.name, s.customer.email, s.customer.vip,
                s.customer.address.street, s.customer.address.city, s.customer.address.country,
                s.customer.address.postalCode));
        for (OrderSpec.LineItemSpec i : s.items) {
            sb.append(line(i.sku, i.name, i.quantity, i.unitPrice, i.discount));
        }
        sb.append(line(s.tags, map(s.attributes)));
        return sb.toString();
    }

    private static String canonical(com.michalklempa.flink.serde.benchmark.pojo.Order o) {
        StringBuilder sb = new StringBuilder();
        sb.append(line(o.orderId, o.createdAtMillis, o.updatedAtMillis, o.status.ordinal(), o.totalAmount, hex(o.payload)));
        sb.append(line(o.customer.customerId, o.customer.name, o.customer.email, o.customer.vip,
                o.customer.address.street, o.customer.address.city, o.customer.address.country,
                o.customer.address.postalCode));
        for (com.michalklempa.flink.serde.benchmark.pojo.LineItem i : o.items) {
            sb.append(line(i.sku, i.name, i.quantity, i.unitPrice, i.discount));
        }
        sb.append(line(o.tags, map(o.attributes)));
        return sb.toString();
    }

    private static String canonical(com.michalklempa.flink.serde.benchmark.pojoplain.Order o) {
        StringBuilder sb = new StringBuilder();
        sb.append(line(o.orderId, o.createdAtMillis, o.updatedAtMillis, o.status.ordinal(), o.totalAmount, hex(o.payload)));
        sb.append(line(o.customer.customerId, o.customer.name, o.customer.email, o.customer.vip,
                o.customer.address.street, o.customer.address.city, o.customer.address.country,
                o.customer.address.postalCode));
        for (com.michalklempa.flink.serde.benchmark.pojoplain.LineItem i : o.items) {
            sb.append(line(i.sku, i.name, i.quantity, i.unitPrice, i.discount));
        }
        sb.append(line(o.tags, map(o.attributes)));
        return sb.toString();
    }

    private static String canonical(com.michalklempa.flink.serde.benchmark.avro.Order o) {
        StringBuilder sb = new StringBuilder();
        java.nio.ByteBuffer buf = o.getPayload().duplicate();
        byte[] payload = new byte[buf.remaining()];
        buf.get(payload);
        sb.append(line(o.getOrderId(), o.getCreatedAtMillis(), o.getUpdatedAtMillis(), o.getStatus().ordinal(),
                o.getTotalAmount(), hex(payload)));
        com.michalklempa.flink.serde.benchmark.avro.Customer c = o.getCustomer();
        sb.append(line(c.getCustomerId(), c.getName(), c.getEmail(), c.getVip(), c.getAddress().getStreet(),
                c.getAddress().getCity(), c.getAddress().getCountry(), c.getAddress().getPostalCode()));
        for (com.michalklempa.flink.serde.benchmark.avro.LineItem i : o.getItems()) {
            sb.append(line(i.getSku(), i.getName(), i.getQuantity(), i.getUnitPrice(), i.getDiscount()));
        }
        List<String> tags = o.getTags();
        sb.append(line(tags, map(o.getAttributes())));
        return sb.toString();
    }

    private static String canonical(com.michalklempa.flink.serde.benchmark.proto.Order o) {
        StringBuilder sb = new StringBuilder();
        sb.append(line(o.getOrderId(), o.getCreatedAtMillis(), o.getUpdatedAtMillis(), o.getStatus().getNumber(),
                o.getTotalAmount(), hex(o.getPayload().toByteArray())));
        com.michalklempa.flink.serde.benchmark.proto.Customer c = o.getCustomer();
        sb.append(line(c.getCustomerId(), c.getName(), c.getEmail(), c.getVip(), c.getAddress().getStreet(),
                c.getAddress().getCity(), c.getAddress().getCountry(), c.getAddress().getPostalCode()));
        for (com.michalklempa.flink.serde.benchmark.proto.LineItem i : o.getItemsList()) {
            sb.append(line(i.getSku(), i.getName(), i.getQuantity(), i.getUnitPrice(), i.getDiscount()));
        }
        sb.append(line(o.getTagsList(), map(o.getAttributesMap())));
        return sb.toString();
    }
}
