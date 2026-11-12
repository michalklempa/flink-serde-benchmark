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
- [x] `SchemaSmokeTest` uses `PojoTestUtils#assertSerializedAsPojoWithoutKryo()` (`flink-test-utils`); `KryoVariantsSmokeTest` asserts the plain POJO is rejected by it
- pipeline.* serialization options: out of scope

## 3. Neutral data generator

Code in `data/`: `Profile`, `OrderSpec`, `OrderGenerator`, `Mappers`. Test: `OrderGeneratorTest`.

- [x] Seeded generator producing a model-independent record description
- [x] Mappers to pojo-hinted, pojo-plain (also used for pojo-kryo), Avro, protobuf
- [x] Profile small (~180 B protobuf)
- [x] Profile medium (~1.8 KB protobuf)
- [x] Profile large (~42 KB protobuf)
- [x] Unit test: all four representations hold the same logical content

## 4. Serializer factories

Code in `serde/`: `SerializerVariant`, `Variants`. Test: `SerializerVariantsTest` (serializer class, generic-types-disabled behaviour, round trip and copy for every variant and profile).

- [x] pojo-hinted: `TypeInformation.of(pojo.Order.class)` gives `PojoSerializer`
- [x] pojo-plain: `TypeInformation.of(pojoplain.Order.class)` gives `PojoSerializer` with Kryo collection fields
- [x] pojo-kryo: `new GenericTypeInfo<>(pojoplain.Order.class)` gives `KryoSerializer`
- [x] Avro: `AvroTypeInfo` for `SpecificRecord` gives `AvroSerializer`
- [x] Protobuf: `ProtoTypeInfo.proto(...)` gives `ProtoSerializer`
- [x] Unit test: each factory yields the expected serializer class; only the two Kryo variants may involve Kryo

## 5. JMH microbenchmarks

Code in `bench/`: `BenchCase` (pre-generated pool of orders (4096 small, 1024 medium, 256 large) and pre-serialized bytes per variant/profile), `SerdeBenchmark` (serialize, deserialize, deserializeReuse, roundTrip, copy; params variant x profile), `SnapshotBenchmark` (snapshot write, read, compatibility check; param variant), `SizeReport` (serialized and snapshot bytes table). Test: `BenchmarkSmokeTest` (runs every operation, the size report, and an embedded JMH run).

- [x] serialize into `DataOutputSerializer`
- [x] deserialize from `DataInputDeserializer`
- [x] round trip
- [x] `copy`
- [x] serialized size per profile
- [x] `-prof gc` allocation rate
- [x] snapshot size (in `SizeReport`) and snapshot write/read/compatibility check cost (`SnapshotBenchmark`)

## Fairness and housekeeping

- [x] Larger object pool sized by profile (4096 / 1024 / 256) instead of 64
- [x] `deserializeReuse` benchmark (`deserialize(reuse, source)`) next to the fresh-object `deserialize`
- [x] README "Methodology and caveats": specific Avro records, untuned Kryo, proto3 defaults, in-memory only
- [ ] Optional sparse-data profile (many default or empty fields), only if the first results look interesting
- [ ] Conclusion paragraph in README after the first real run

## 6. Flink jobs (mini cluster, parallelism 1)

Two jobs, each run per variant (pojo-hinted, pojo-plain, pojo-kryo, avro, protobuf) and profile. Code in `job/`, started from a `main` method that builds a local environment (mini cluster in the JVM). Parallelism 1, max parallelism 128, operator chaining disabled so every hop goes through the serializer, checkpointing off during throughput runs, `disableGenericTypes()` on for the three non-Kryo variants.

- Job A, operator-to-operator: `source -> pass-through map -> sink`. Measures the network-path (record) serde only.
- Job B, keyed state: `source(Tuple2<key, order>) -> keyBy -> KeyedProcessFunction (read-modify-write ValueState<order>) -> sink`. Measures record serde plus state serde. Job B minus Job A is the state cost.

Decisions are in "6.0 Decisions" below.

### 6.0 Decisions

- [x] Throughput measurement: sink timestamps at record W and N (accumulator), throughput = (N-W)/elapsed
- [x] JVM isolation: one JVM per run, started by the driver with `ProcessBuilder`
- [x] Backends: throughput on RocksDB only; heap and RocksDB both in the later size / restore phase (6.6)
- [x] Checkpoint size / restore phase (6.6): later, after 6.1-6.5
- [x] Parameters (`full`): N = 2,400,000 small / 600,000 medium / 35,000 large (sized for about 60 min), W = 10 percent of N, K = 100,000 small / 10,000 medium / 1,000 large, 3 repeats, median. `quick`: N = 300,000 / 75,000 / 4,500, 1 repeat (about 5 min). `smoke`: tiny, for tests.

### 6.1 Dependencies and scaffold

Implemented in `job/`: `JobConfig`, `JobResult`, `PoolGenerator`, `KeyedPoolGenerator`, `TimingSink`, `KeyedStateFunction`, `JobRunner` (main for one run), `JobBenchmarkMain` (driver) and `JobReportBuilder`. Tests: `JobSmokeTest` (both jobs for all 5 variants, config parsing, driver in smoke mode).

- [x] Add `flink-streaming-java`, `flink-clients`, `flink-runtime`, `flink-connector-datagen`, `flink-statebackend-rocksdb` (1.18.1) to `pom.xml`
- [x] Check the shaded `benchmarks.jar` still starts JMH and the job main (service files, log4j)
- [x] `job/JobConfig`: variant, profile, N, W, K, backend, job kind; parsed from `main` arguments

### 6.2 Shared building blocks

- [x] Source: `DataGeneratorSource` with a `GeneratorFunction` cycling the pre-built pool from `BenchCase`, bounded to N records, no rate limit, explicit `TypeInformation` from the variant
- [x] For Job B the source emits `Tuple2<Long, T>` with `Types.TUPLE(Types.LONG, variantTypeInfo)` and key = index modulo K
- [x] `TimingSink`: counts records, writes `System.nanoTime()` into an accumulator at record W and at record N
- [x] Result object: records, elapsed nanos between W and N, throughput records/s and MB/s (serialized bytes), printed as one JSON line

### 6.3 Job A (operator to operator)

- [x] Pass-through operator (identity map) with chaining disabled between source, map and sink
- [x] Unit test per variant: small run through the mini cluster, sink saw every record (`JobSmokeTest`)

### 6.4 Job B (keyed state)

- [x] `KeyedProcessFunction` with `ValueState<T>` created from the variant `TypeInformation` (not a class), read-modify-write per record
- [x] RocksDB backend with a local working directory
- (heap backend is only used in 6.6)
- [x] Unit test per variant: small run through the mini cluster, sink saw every record (state content is not asserted; the serializer round trips are covered by `SerializerVariantsTest`)

### 6.5 Driver and reporting

- [x] `JobBenchmarkMain` driver: for each (job, variant, profile, backend, repeat) start a JVM with `ProcessBuilder`, read the JSON line, take the median over repeats
- [x] Render markdown tables (records/s per variant and profile, Job A, Job B, and Job B minus Job A as cost per record) and ASCII bars between a second pair of README markers (`jobs:start` / `jobs:end`)
- [x] Smoke mode for tests (tiny N, one variant)
- [x] Document run modes and durations in `README.md`

### 6.6 State size and restore (later, after 6.1-6.5)

- [ ] Unbounded Job B until K keys are loaded, `JobClient.stopWithSavepoint`
- [ ] Savepoint directory size in bytes per variant and backend
- [ ] Restart from the savepoint, time to first processed record
- [ ] Report table

## 7. Results

Generated in Java (no Python): `bench/ReportMain` runs JMH programmatically and `bench/ReportBuilder` renders markdown tables and ASCII bar charts into the marked block of `README.md`. Test: `BenchmarkSmokeTest#reportMainRewritesReadmeBlock` (smoke mode).

- [x] Programmatic JMH run with GC profiler (`ReportMain quick|full`)
- [x] Markdown tables (ns/op, B/op, serialized bytes, snapshot) and ASCII bar charts, written into README between markers
- [x] Document run modes and durations in `README.md`
- [x] Do the first real `quick` run and review the numbers (2026-11-12, results in README)
- [ ] Do a `full` run, record JVM flags and CPU pinning in `README.md`
- [ ] Do the first real `quick` run of `JobBenchmarkMain` and review
