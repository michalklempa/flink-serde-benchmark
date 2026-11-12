package com.michalklempa.flink.serde.benchmark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.michalklempa.flink.serde.benchmark.pojo.Order;
import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.junit.jupiter.api.Test;

class SchemaSmokeTest {

    @Test
    void pojoIsRecognizedWithoutGenericTypes() {
        TypeInformation<Order> info = TypeInformation.of(Order.class);
        assertTrue(info instanceof PojoTypeInfo);
        assertFalse(containsGeneric(info), info.toString());
    }

    @Test
    void avroOrderBuilds() {
        com.michalklempa.flink.serde.benchmark.avro.Order order = com.michalklempa.flink.serde.benchmark.avro.Order
                .newBuilder()
                .setOrderId("o-1")
                .setCreatedAtMillis(1L)
                .setUpdatedAtMillis(2L)
                .setStatus(com.michalklempa.flink.serde.benchmark.avro.OrderStatus.NEW)
                .setCustomer(com.michalklempa.flink.serde.benchmark.avro.Customer.newBuilder()
                        .setCustomerId(7L)
                        .setName("n")
                        .setEmail("e")
                        .setVip(true)
                        .setAddress(com.michalklempa.flink.serde.benchmark.avro.Address.newBuilder()
                                .setStreet("s")
                                .setCity("c")
                                .setCountry("k")
                                .setPostalCode("p")
                                .build())
                        .build())
                .setItems(List.of())
                .setTags(List.of("a"))
                .setAttributes(java.util.Map.of("k", "v"))
                .setTotalAmount(1.5)
                .setPayload(java.nio.ByteBuffer.wrap(new byte[] {1}))
                .build();
        assertEquals("o-1", order.getOrderId());
    }

    @Test
    void protoOrderBuilds() {
        com.michalklempa.flink.serde.benchmark.proto.Order order = com.michalklempa.flink.serde.benchmark.proto.Order
                .newBuilder()
                .setOrderId("o-1")
                .setStatus(com.michalklempa.flink.serde.benchmark.proto.OrderStatus.PAID)
                .setCustomer(com.michalklempa.flink.serde.benchmark.proto.Customer.newBuilder().setCustomerId(7L))
                .addTags("a")
                .putAttributes("k", "v")
                .build();
        assertEquals(1, order.getTagsCount());
    }

    private static boolean containsGeneric(TypeInformation<?> info) {
        if (info instanceof GenericTypeInfo) {
            return true;
        }
        if (info instanceof ListTypeInfo) {
            return containsGeneric(((ListTypeInfo<?>) info).getElementTypeInfo());
        }
        if (info instanceof MapTypeInfo) {
            MapTypeInfo<?, ?> m = (MapTypeInfo<?, ?>) info;
            return containsGeneric(m.getKeyTypeInfo()) || containsGeneric(m.getValueTypeInfo());
        }
        if (info instanceof PojoTypeInfo) {
            PojoTypeInfo<?> p = (PojoTypeInfo<?>) info;
            for (int i = 0; i < p.getArity(); i++) {
                PojoField f = p.getPojoFieldAt(i);
                if (containsGeneric(f.getTypeInformation())) {
                    return true;
                }
            }
        }
        return false;
    }
}
