# PLAN

Flink state serde benchmark: POJO (`PojoSerializer`) vs Protobuf (`ProtoSerializer` from `flink-protobuf`) vs Avro (`AvroSerializer`) on identical data.

## Constraints

- Java 11, Flink 1.18.1, JMH.
- Depends on `com.michalklempa:flink-protobuf-serde:1.1.0-1.18` (run `mvn install` in `../flink-protobuf/flink-protobuf-serde` first).
- Independent Maven project, no parent pom.
- `disableGenericTypes()` on for the pojo-hinted, avro and protobuf variants so Kryo cannot be used by accident. The two Kryo variants below are the deliberate exception and are expected to fail under it.
- No `CLAUDE.md` in this directory; Claude guidance goes in a "Notes for Claude" section of `README.md`.

## Variants under comparison

| Variant | Classes | Type info | Serializer |
|---|---|---|---|
| pojo-hinted | `pojo.Order` with `@TypeInfo` on collections | `PojoTypeInfo`, no generic fields | `PojoSerializer` |
| pojo-plain (trap) | `pojoplain.Order`, no annotations | `PojoTypeInfo`, `List`/`Map` fields are `GenericTypeInfo` | `PojoSerializer` with Kryo per collection field |
| pojo-kryo | `pojoplain.Order` | `GenericTypeInfo` | `KryoSerializer` for the whole object |
| avro | generated `avro.Order` | `AvroTypeInfo` | `AvroSerializer` |
| protobuf | generated `proto.Order` | `ProtoTypeInfo` | `ProtoSerializer` |

pojo-plain simulates a trap a developer falls into unintentionally; pojo-kryo is the worst-case baseline. Both throw under `disableGenericTypes()`.

## Decisions

- [x] POJO collections: hinted collection type info only, fair comparison, no Kryo
- [x] Scope: steps 1-4 first with small test runs; each serializer needs a compilable, runnable example (unit test)
- [x] No schema evolution, steady-state only
- [x] Record shape: synthetic order-like event (nested customer, repeated line items, tags, attributes map, enum status, timestamps)

## Side tasks

- [x] Kryo fallback for `List`/`Map` fields: resolved. In Flink 1.18.1 `TypeExtractor.privateGetForClass` (TypeExtractor.java ~l.1942) returns `GenericTypeInfo` for any interface, so `List`, `Map`, `Set` fields (even of POJOs or Strings) are Kryo-serialized as a whole; no list/map sub-serializer is picked automatically. `analyzePojo` (~l.2105) only logs "Field ... will be processed as GenericType". Only arrays (`ObjectArrayTypeInfo`) and `@TypeInfo`-hinted fields get proper serializers. Verified by probe: `List<Item>`, `List<String>`, `Map<String,String>`, `Set<String>` -> `GenericTypeInfo`; `Item[]` -> `ObjectArrayTypeInfo`.

## 1. Scaffold

- [x] `pom.xml` with Flink 1.18.1, JMH, `flink-protobuf-serde`, `flink-avro`
- [x] `avro-maven-plugin` for Avro class generation
- [x] pinned `protoc.exe` via `maven-dependency-plugin` + `maven-antrun-plugin` + `build-helper` for protobuf class generation
- [x] shade plugin for the JMH uber jar
- [x] `.sdkmanrc` / `.envrc` matching `flink-protobuf`
- [x] `README.md` with "Notes for Claude" section

## 2. Schema in three forms

protoc is pinned via Maven (`maven-dependency-plugin` + `maven-antrun-plugin` + `build-helper`), as in the michalklempa.com post "Pinning protoc and protobuf-java version with Maven". `.proto` lives in `src/main/resources/proto`.

Common feature subset: int, long, double, boolean, string, bytes, enum, nested record, repeated nested records, repeated string, map<string, X>, timestamp as epoch-millis long. No `oneof`, no Avro unions, no nulls.

- [x] `order.proto`
- [x] `order.avsc`
- [x] POJO class(es) obeying Flink POJO rules (public no-arg constructor, public fields or getters/setters)
- [x] hinted collection type info on the POJO (`@TypeInfo` factories for `List`/`Map`)
- [x] `pojoplain` package: same POJO classes without annotations (pojo-plain and pojo-kryo variants)
- [x] `KryoVariantsSmokeTest`: plain POJO has Kryo collection fields and fails under `disableGenericTypes()`; full Kryo round trips through `KryoSerializer`
- [ ] Switch `SchemaSmokeTest` to `PojoTestUtils#assertSerializedAsPojoWithoutKryo()` (needs `flink-test-utils`)
- [ ] Check which `pipeline.*` serialization options exist in the 1.18 docs

## 3. Neutral data generator

- [ ] Seeded generator producing a model-independent record description
- [ ] Mappers to pojo-hinted, pojo-plain (also used for pojo-kryo), Avro, protobuf
- [ ] Profile small (~100 B)
- [ ] Profile medium (~2 KB)
- [ ] Profile large (~50 KB)
- [ ] Unit test: all three representations hold the same logical content

## 4. Serializer factories

- [ ] pojo-hinted: `TypeInformation.of(pojo.Order.class)` gives `PojoSerializer`
- [ ] pojo-plain: `TypeInformation.of(pojoplain.Order.class)` gives `PojoSerializer` with Kryo collection fields
- [ ] pojo-kryo: `new GenericTypeInfo<>(pojoplain.Order.class)` gives `KryoSerializer`
- [ ] Avro: `AvroTypeInfo` for `SpecificRecord` gives `AvroSerializer`
- [ ] Protobuf: `ProtoTypeInfo.proto(...)` gives `ProtoSerializer`
- [ ] Unit test: each factory yields the expected serializer class; only the two Kryo variants may involve Kryo

## 5. JMH microbenchmarks

- [ ] serialize into `DataOutputSerializer`
- [ ] deserialize from `DataInputDeserializer`
- [ ] round trip
- [ ] `copy`
- [ ] serialized size per profile
- [ ] `-prof gc` allocation rate
- [ ] snapshot size and compatibility check cost

## 6. State-level end-to-end job

- [ ] Bounded generator source
- [ ] `ValueState` per type
- [ ] `MapState` per type
- [ ] RocksDB backend
- [ ] Heap backend
- [ ] Measure throughput, checkpoint/savepoint size, restore time

## 7. Results

- [ ] JMH JSON output
- [ ] Script producing comparison table (ops/s, bytes, B/op)
- [ ] Document JVM flags, CPU pinning, how to reproduce in `README.md`
