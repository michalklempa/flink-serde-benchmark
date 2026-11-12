# flink-serde-benchmark

Flink state serializer benchmark: POJO (`PojoSerializer`) vs Protobuf (`ProtoSerializer` from [`flink-protobuf`](../flink-protobuf)) vs Avro (`AvroSerializer`) on the same logical data.

See [PLAN.md](PLAN.md) for status.

## Build

```bash
mvn -f ../flink-protobuf/flink-protobuf-serde/pom.xml install
mvn clean package
```

## Run

```bash
mvn clean package
java -cp target/benchmarks.jar com.michalklempa.flink.serde.benchmark.bench.ReportMain quick
```

`ReportMain [quick|full] [readme-path]` runs every benchmark through the JMH `Runner` API with the GC profiler and rewrites the block between the `results:start` / `results:end` markers in the README: markdown tables plus ASCII bar charts (plain block characters in a code block, readable anywhere). Nothing but the JDK and this jar is needed.

### Run durations

`SerdeBenchmark` has 5 methods x 5 variants x 3 profiles = 75 benchmarks, `SnapshotBenchmark` has 5, so 80 in total. Duration is roughly `benchmarks x forks x (warmup + measurement iterations x time + ~1 s fork startup)`.

| Mode | Settings | Per benchmark | Total |
|---|---|---|---|
| `quick` | 1 fork, 1 x 1 s warmup, 2 x 1 s measurement | ~4 s | ~5.5 min |
| `full` | 2 forks, 5 x 1 s warmup, 5 x 1 s measurement | ~22 s | ~30 min |

The `quick` run is for a first comparison and sanity check; one fork and two 1 s iterations give noisy scores, so use `full` for numbers you publish. The durations are estimates from the formula, not timed runs.

Raw JMH CLI is still available for ad-hoc runs, for example:
`java -jar target/benchmarks.jar SerdeBenchmark.serialize -p profile=SMALL -f 1 -wi 1 -i 2 -w 1 -r 1 -prof gc`.

## Methodology and caveats

- Only the `TypeSerializer` is measured, in memory, with a reused `DataOutputSerializer` / `DataInputDeserializer`. No state backend, network or checkpointing is involved.
- Each benchmark cycles through a pool of pre-generated orders (4096 small, 1024 medium, 256 large) so the JIT cannot specialise on one object and the working set is larger than L1/L2 cache. Generation and mapping are outside the timed code.
- `deserialize` creates a new object every call; `deserializeReuse` calls `deserialize(reuse, source)` with one object, as Flink does when object reuse is enabled. Protobuf messages and Kryo ignore the reuse object, so for them both numbers match.
- Avro uses generated specific records: classes generated from `order.avsc` by `avro-maven-plugin` (`SpecificRecord`, strings as `java.lang.String`), wrapped by `AvroTypeInfo` and serialized by Flink's `AvroSerializer`. Generic records and reflect-based POJOs are not benchmarked. The serializer snapshot carries the full Avro schema, which is why Avro's snapshot is larger than protobuf's.
- `pojo-plain` and `pojo-kryo` use Flink's default Kryo with no class registration, which is what an unaware developer gets; a tuned Kryo setup would be faster and smaller. All other variants run with `disableGenericTypes()`.
- Protobuf is proto3, which does not write default values. The generated data rarely contains zeros or empty strings, so this has little effect on sizes here.

## Results

<!-- results:start -->
Mode `quick`: 1 fork, 1 x 1 s warmup, 2 x 1 s measurement. Scores are noisy.
Java 11.0.29, Linux, 8 CPUs, run on 2026-11-12.

#### serialize (ns/op, lower is better)

| variant | SMALL | MEDIUM | LARGE |
|---|---:|---:|---:|
| protobuf | 520 | 3,746 | 59,832 |
| avro | 757 | 4,375 | 70,066 |
| pojo-hinted | 1,216 | 5,476 | 95,190 |
| pojo-plain | 2,369 | 9,053 | 128,475 |
| pojo-kryo | 2,851 | 9,795 | 129,224 |

```
serialize SMALL (ns/op)
protobuf     █████░░░░░░░░░░░░░░░░░░░░░░░░░          520
avro         ████████░░░░░░░░░░░░░░░░░░░░░░          757
pojo-hinted  █████████████░░░░░░░░░░░░░░░░░        1,216
pojo-plain   █████████████████████████░░░░░        2,369
pojo-kryo    ██████████████████████████████        2,851

serialize MEDIUM (ns/op)
protobuf     ███████████░░░░░░░░░░░░░░░░░░░        3,746
avro         █████████████░░░░░░░░░░░░░░░░░        4,375
pojo-hinted  █████████████████░░░░░░░░░░░░░        5,476
pojo-plain   ████████████████████████████░░        9,053
pojo-kryo    ██████████████████████████████        9,795

serialize LARGE (ns/op)
protobuf     ██████████████░░░░░░░░░░░░░░░░       59,832
avro         ████████████████░░░░░░░░░░░░░░       70,066
pojo-hinted  ██████████████████████░░░░░░░░       95,190
pojo-plain   ██████████████████████████████      128,475
pojo-kryo    ██████████████████████████████      129,224
```

#### deserialize (new object each time) (ns/op, lower is better)

| variant | SMALL | MEDIUM | LARGE |
|---|---:|---:|---:|
| protobuf | 1,127 | 6,719 | 108,253 |
| pojo-hinted | 1,636 | 7,794 | 147,031 |
| avro | 2,050 | 11,772 | 189,848 |
| pojo-plain | 4,185 | 19,748 | 379,781 |
| pojo-kryo | 5,697 | 21,080 | 388,202 |

```
deserialize SMALL (ns/op)
protobuf     ██████░░░░░░░░░░░░░░░░░░░░░░░░        1,127
pojo-hinted  █████████░░░░░░░░░░░░░░░░░░░░░        1,636
avro         ███████████░░░░░░░░░░░░░░░░░░░        2,050
pojo-plain   ██████████████████████░░░░░░░░        4,185
pojo-kryo    ██████████████████████████████        5,697

deserialize MEDIUM (ns/op)
protobuf     ██████████░░░░░░░░░░░░░░░░░░░░        6,719
pojo-hinted  ███████████░░░░░░░░░░░░░░░░░░░        7,794
avro         █████████████████░░░░░░░░░░░░░       11,772
pojo-plain   ████████████████████████████░░       19,748
pojo-kryo    ██████████████████████████████       21,080

deserialize LARGE (ns/op)
protobuf     ████████░░░░░░░░░░░░░░░░░░░░░░      108,253
pojo-hinted  ███████████░░░░░░░░░░░░░░░░░░░      147,031
avro         ███████████████░░░░░░░░░░░░░░░      189,848
pojo-plain   █████████████████████████████░      379,781
pojo-kryo    ██████████████████████████████      388,202
```

#### deserialize (reusing one object) (ns/op, lower is better)

| variant | SMALL | MEDIUM | LARGE |
|---|---:|---:|---:|
| protobuf | 1,064 | 7,693 | 115,947 |
| pojo-hinted | 1,199 | 7,410 | 147,869 |
| avro | 2,037 | 9,681 | 159,866 |
| pojo-plain | 4,063 | 19,544 | 380,658 |
| pojo-kryo | 5,788 | 20,996 | 402,014 |

```
deserializeReuse SMALL (ns/op)
protobuf     ██████░░░░░░░░░░░░░░░░░░░░░░░░        1,064
pojo-hinted  ██████░░░░░░░░░░░░░░░░░░░░░░░░        1,199
avro         ███████████░░░░░░░░░░░░░░░░░░░        2,037
pojo-plain   █████████████████████░░░░░░░░░        4,063
pojo-kryo    ██████████████████████████████        5,788

deserializeReuse MEDIUM (ns/op)
protobuf     ███████████░░░░░░░░░░░░░░░░░░░        7,693
pojo-hinted  ███████████░░░░░░░░░░░░░░░░░░░        7,410
avro         ██████████████░░░░░░░░░░░░░░░░        9,681
pojo-plain   ████████████████████████████░░       19,544
pojo-kryo    ██████████████████████████████       20,996

deserializeReuse LARGE (ns/op)
protobuf     █████████░░░░░░░░░░░░░░░░░░░░░      115,947
pojo-hinted  ███████████░░░░░░░░░░░░░░░░░░░      147,869
avro         ████████████░░░░░░░░░░░░░░░░░░      159,866
pojo-plain   ████████████████████████████░░      380,658
pojo-kryo    ██████████████████████████████      402,014
```

#### round trip (serialize + deserialize) (ns/op, lower is better)

| variant | SMALL | MEDIUM | LARGE |
|---|---:|---:|---:|
| protobuf | 1,911 | 10,324 | 161,671 |
| pojo-hinted | 2,772 | 14,845 | 241,230 |
| avro | 3,218 | 16,255 | 265,417 |
| pojo-plain | 6,917 | 30,288 | 513,250 |
| pojo-kryo | 9,120 | 31,692 | 521,052 |

```
roundTrip SMALL (ns/op)
protobuf     ██████░░░░░░░░░░░░░░░░░░░░░░░░        1,911
pojo-hinted  █████████░░░░░░░░░░░░░░░░░░░░░        2,772
avro         ███████████░░░░░░░░░░░░░░░░░░░        3,218
pojo-plain   ███████████████████████░░░░░░░        6,917
pojo-kryo    ██████████████████████████████        9,120

roundTrip MEDIUM (ns/op)
protobuf     ██████████░░░░░░░░░░░░░░░░░░░░       10,324
pojo-hinted  ██████████████░░░░░░░░░░░░░░░░       14,845
avro         ███████████████░░░░░░░░░░░░░░░       16,255
pojo-plain   █████████████████████████████░       30,288
pojo-kryo    ██████████████████████████████       31,692

roundTrip LARGE (ns/op)
protobuf     █████████░░░░░░░░░░░░░░░░░░░░░      161,671
pojo-hinted  ██████████████░░░░░░░░░░░░░░░░      241,230
avro         ███████████████░░░░░░░░░░░░░░░      265,417
pojo-plain   ██████████████████████████████      513,250
pojo-kryo    ██████████████████████████████      521,052
```

#### copy (ns/op, lower is better)

| variant | SMALL | MEDIUM | LARGE |
|---|---:|---:|---:|
| protobuf | 7.5 | 4.8 | 4.6 |
| pojo-hinted | 1,082 | 4,552 | 76,238 |
| avro | 982 | 4,962 | 82,120 |
| pojo-plain | 1,767 | 9,601 | 189,968 |
| pojo-kryo | 1,990 | 11,206 | 251,275 |

```
copy SMALL (ns/op)
protobuf     █░░░░░░░░░░░░░░░░░░░░░░░░░░░░░          7.5
pojo-hinted  ████████████████░░░░░░░░░░░░░░        1,082
avro         ███████████████░░░░░░░░░░░░░░░          982
pojo-plain   ███████████████████████████░░░        1,767
pojo-kryo    ██████████████████████████████        1,990

copy MEDIUM (ns/op)
protobuf     █░░░░░░░░░░░░░░░░░░░░░░░░░░░░░          4.8
pojo-hinted  ████████████░░░░░░░░░░░░░░░░░░        4,552
avro         █████████████░░░░░░░░░░░░░░░░░        4,962
pojo-plain   ██████████████████████████░░░░        9,601
pojo-kryo    ██████████████████████████████       11,206

copy LARGE (ns/op)
protobuf     █░░░░░░░░░░░░░░░░░░░░░░░░░░░░░          4.6
pojo-hinted  █████████░░░░░░░░░░░░░░░░░░░░░       76,238
avro         ██████████░░░░░░░░░░░░░░░░░░░░       82,120
pojo-plain   ███████████████████████░░░░░░░      189,968
pojo-kryo    ██████████████████████████████      251,275
```

#### allocation per round trip (B/op, lower is better)

| variant | SMALL | MEDIUM | LARGE |
|---|---:|---:|---:|
| protobuf | 2,386 | 13,183 | 247,386 |
| pojo-hinted | 1,502 | 8,152 | 157,113 |
| avro | 2,308 | 13,392 | 251,339 |
| pojo-plain | 1,726 | 6,616 | 110,323 |
| pojo-kryo | 1,566 | 6,456 | 110,163 |

#### serialized size (bytes per order, lower is better)

| variant | SMALL | MEDIUM | LARGE |
|---|---:|---:|---:|
| pojo-hinted | 213 | 1,847 | 44,143 |
| pojo-plain | 321 | 1,871 | 41,396 |
| pojo-kryo | 385 | 1,907 | 41,224 |
| avro | 214 | 1,924 | 44,757 |
| protobuf | 182 | 1,787 | 42,327 |

#### serializer snapshot (written into every checkpoint)

| variant | snapshot bytes | write + read + compatibility check (µs/op) |
|---|---:|---:|
| pojo-hinted | 4,959 | 81.5 |
| pojo-plain | 5,867 | 116 |
| pojo-kryo | 943 | 17.7 |
| avro | 1,769 | 46.3 |
| protobuf | 113 | 1.7 |


<!-- results:end -->

## Notes for Claude

- Independent Maven project, no parent pom. Java 11, Flink 1.18.1.
- `flink-protobuf-serde` comes from the local `~/.m2`; install it from the sibling directory first.
- Avro classes are generated from `src/main/resources/avro`, protobuf classes from `src/main/resources/proto` using a pinned `protoc.exe` downloaded from Maven Central (no local protoc needed); both land in `target/generated-sources`.
- The POJO `Order` hints `List`/`Map` fields with `@TypeInfo` factories; `SchemaSmokeTest` fails if any field resolves to `GenericTypeInfo` (Kryo).
- Five variants (pojo-hinted, pojo-plain, pojo-kryo, avro, protobuf); see PLAN.md. Only pojo-plain and pojo-kryo may use Kryo, the rest must work with generic types disabled.
