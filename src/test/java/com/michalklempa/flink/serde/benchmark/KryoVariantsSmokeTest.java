package com.michalklempa.flink.serde.benchmark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.michalklempa.flink.serde.benchmark.pojoplain.Order;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;

class KryoVariantsSmokeTest {

    @Test
    void plainPojoIsPojoWithKryoCollectionFields() {
        PojoTypeInfo<Order> info = assertInstanceOf(PojoTypeInfo.class, TypeInformation.of(Order.class));
        for (String name : new String[] {"items", "tags", "attributes"}) {
            int idx = info.getFieldIndex(name);
            assertInstanceOf(GenericTypeInfo.class, info.getPojoFieldAt(idx).getTypeInformation(), name);
        }
        for (String name : new String[] {"orderId", "customer", "status", "payload", "totalAmount"}) {
            int idx = info.getFieldIndex(name);
            assertEquals(false, info.getPojoFieldAt(idx).getTypeInformation() instanceof GenericTypeInfo, name);
        }
        assertInstanceOf(PojoSerializer.class, info.createSerializer(new ExecutionConfig()));
    }

    @Test
    void plainPojoFailsFastWhenGenericTypesDisabled() {
        ExecutionConfig config = new ExecutionConfig();
        config.disableGenericTypes();
        TypeInformation<Order> info = TypeInformation.of(Order.class);
        assertThrows(UnsupportedOperationException.class, () -> info.createSerializer(config));
    }

    @Test
    void fullKryoUsesKryoSerializerAndRoundTrips() throws Exception {
        TypeInformation<Order> info = new GenericTypeInfo<>(Order.class);
        TypeSerializer<Order> serializer = info.createSerializer(new ExecutionConfig());
        assertInstanceOf(KryoSerializer.class, serializer);

        Order order = new Order();
        order.orderId = "o-1";
        order.items = new ArrayList<>();
        order.tags = new ArrayList<>(java.util.List.of("a"));
        order.attributes = new HashMap<>(java.util.Map.of("k", "v"));
        order.payload = new byte[] {1, 2};

        DataOutputSerializer out = new DataOutputSerializer(64);
        serializer.serialize(order, out);
        Order back = serializer.deserialize(new DataInputDeserializer(out.getCopyOfBuffer()));
        assertEquals("o-1", back.orderId);
        assertEquals(order.tags, back.tags);
        assertEquals(order.attributes, back.attributes);
    }

    @Test
    void fullKryoFailsFastWhenGenericTypesDisabled() {
        ExecutionConfig config = new ExecutionConfig();
        config.disableGenericTypes();
        TypeInformation<Order> info = new GenericTypeInfo<>(Order.class);
        assertThrows(UnsupportedOperationException.class, () -> info.createSerializer(config));
    }
}
