package com.michalklempa.flink.serde.benchmark.serde;

import com.michalklempa.flink.protobuf.ProtoSerializer;
import com.michalklempa.flink.protobuf.ProtoTypeInfo;
import com.michalklempa.flink.serde.benchmark.data.Mappers;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.formats.avro.typeutils.AvroSerializer;
import org.apache.flink.formats.avro.typeutils.AvroTypeInfo;

public final class Variants {
    private Variants() {
    }

    public static SerializerVariant<com.michalklempa.flink.serde.benchmark.pojo.Order> pojoHinted() {
        return new SerializerVariant<>("pojo-hinted",
                TypeInformation.of(com.michalklempa.flink.serde.benchmark.pojo.Order.class),
                Mappers::toPojo, PojoSerializer.class, false);
    }

    public static SerializerVariant<com.michalklempa.flink.serde.benchmark.pojoplain.Order> pojoPlain() {
        return new SerializerVariant<>("pojo-plain",
                TypeInformation.of(com.michalklempa.flink.serde.benchmark.pojoplain.Order.class),
                Mappers::toPojoPlain, PojoSerializer.class, true);
    }

    public static SerializerVariant<com.michalklempa.flink.serde.benchmark.pojoplain.Order> pojoKryo() {
        return new SerializerVariant<>("pojo-kryo",
                new GenericTypeInfo<>(com.michalklempa.flink.serde.benchmark.pojoplain.Order.class),
                Mappers::toPojoPlain, KryoSerializer.class, true);
    }

    public static SerializerVariant<com.michalklempa.flink.serde.benchmark.avro.Order> avro() {
        return new SerializerVariant<>("avro",
                new AvroTypeInfo<>(com.michalklempa.flink.serde.benchmark.avro.Order.class),
                Mappers::toAvro, AvroSerializer.class, false);
    }

    public static SerializerVariant<com.michalklempa.flink.serde.benchmark.proto.Order> protobuf() {
        return new SerializerVariant<>("protobuf",
                ProtoTypeInfo.proto(com.michalklempa.flink.serde.benchmark.proto.Order.class),
                Mappers::toProto, ProtoSerializer.class, false);
    }

    public static List<SerializerVariant<?>> all() {
        return Arrays.asList(pojoHinted(), pojoPlain(), pojoKryo(), avro(), protobuf());
    }
}
