# flink-serde-benchmark

Flink state serializer benchmark: POJO (`PojoSerializer`) vs Protobuf (`ProtoSerializer` from [`flink-protobuf`](../flink-protobuf)) vs Avro (`AvroSerializer`) on the same logical data.

See [PLAN.md](PLAN.md) for status.

## Build

```bash
mvn -f ../flink-protobuf/flink-protobuf-serde/pom.xml install
mvn clean package
```

## Notes for Claude

- Independent Maven project, no parent pom. Java 11, Flink 1.18.1.
- `flink-protobuf-serde` comes from the local `~/.m2`; install it from the sibling directory first.
- Avro classes are generated from `src/main/resources/avro`, protobuf classes from `src/main/resources/proto` using a pinned `protoc.exe` downloaded from Maven Central (no local protoc needed); both land in `target/generated-sources`.
- The POJO `Order` hints `List`/`Map` fields with `@TypeInfo` factories; `SchemaSmokeTest` fails if any field resolves to `GenericTypeInfo` (Kryo).
- Five variants (pojo-hinted, pojo-plain, pojo-kryo, avro, protobuf); see PLAN.md. Only pojo-plain and pojo-kryo may use Kryo, the rest must work with generic types disabled.
