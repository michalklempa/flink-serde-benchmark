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

## Flink jobs

Two jobs run on a mini cluster in a single JVM with parallelism 1, max parallelism 128 and operator chaining off, so every hop goes through the serializer:

- Job A: `source -> pass-through map -> sink` (record serde between operators).
- Job B: `source -> keyBy -> KeyedProcessFunction with ValueState<Order> (read, then update) -> sink`, on the RocksDB state backend. Job B minus Job A is the state cost per record.

Throughput is measured in the sink: it stores `System.nanoTime()` at record W (10 percent of N) and at record N into accumulators, and throughput is `(N - W) / elapsed`, which excludes job startup and warmup. Every run is a separate JVM started by the driver; the median over the repeats is reported.

```bash
java -cp target/benchmarks.jar com.michalklempa.flink.serde.benchmark.job.JobBenchmarkMain quick
java -cp target/benchmarks.jar com.michalklempa.flink.serde.benchmark.job.JobBenchmarkMain full
```

| Mode | Records N (small / medium / large) | Keys K | Repeats | Runs | Estimated time |
|---|---|---|---|---|---|
| `quick` | 300,000 / 75,000 / 4,500 | 100,000 / 10,000 / 1,000 | 1 | 30 | about 5 min |
| `full` | 2,400,000 / 600,000 / 35,000 | 100,000 / 10,000 / 1,000 | 3 | 90 | about 60 min |

Sizing: a run costs about 5.5 s of JVM and mini cluster startup plus `N / throughput`. Measured throughput on the dev machine (records/s, fastest and slowest variant): Job A small 239k / 103k, medium 44k / 21k, large 2.3k / 1.2k; Job B small 74k / 31k, medium 19k / 8k, large 970 / 608. N is chosen so each profile takes about the same share of time, which gives about 20 minutes per repeat of the full matrix (about 5 minutes for `quick`). The totals are estimates from these rates, not a timed run; the Kryo variants take about twice as long as the others.

A single run: `java -cp target/benchmarks.jar com.michalklempa.flink.serde.benchmark.job.JobRunner --job=B --variant=protobuf --profile=MEDIUM --records=50000 --keys=10000`.

## Methodology and caveats

- Only the `TypeSerializer` is measured, in memory, with a reused `DataOutputSerializer` / `DataInputDeserializer`. No state backend, network or checkpointing is involved.
- Each benchmark cycles through a pool of pre-generated orders (4096 small, 1024 medium, 256 large) so the JIT cannot specialise on one object and the working set is larger than L1/L2 cache. Generation and mapping are outside the timed code.
- `deserialize` creates a new object every call; `deserializeReuse` calls `deserialize(reuse, source)` with one object, as Flink does when object reuse is enabled. Protobuf messages and Kryo ignore the reuse object, so for them both numbers match.
- Avro uses generated specific records: classes generated from `order.avsc` by `avro-maven-plugin` (`SpecificRecord`, strings as `java.lang.String`), wrapped by `AvroTypeInfo` and serialized by Flink's `AvroSerializer`. Generic records and reflect-based POJOs are not benchmarked. The serializer snapshot carries the full Avro schema, which is why Avro's snapshot is larger than protobuf's.
- `pojo-plain` and `pojo-kryo` use Flink's default Kryo with no class registration, which is what an unaware developer gets; a tuned Kryo setup would be faster and smaller. All other variants run with `disableGenericTypes()`.
- Protobuf is proto3, which does not write default values. The generated data rarely contains zeros or empty strings, so this has little effect on sizes here.

## Results

<!-- results:start -->
Mode `full`: 2 forks, 5 x 1 s warmup, 5 x 1 s measurement.
Java 11.0.29, Linux, 8 CPUs, run on 2026-11-12.

#### serialize (ns/op, lower is better)

| variant | SMALL | MEDIUM | LARGE |
|---|---:|---:|---:|
| protobuf | 511 ± 34.9 | 3,749 ± 213 | 57,510 ± 2,880 |
| avro | 794 ± 70.7 | 4,205 ± 222 | 73,799 ± 2,782 |
| pojo-hinted | 651 ± 35.9 | 5,623 ± 602 | 96,239 ± 5,336 |
| pojo-kryo | 2,654 ± 324 | 10,026 ± 311 | 129,784 ± 8,815 |
| pojo-plain | 1,438 ± 64.6 | 8,534 ± 827 | 133,448 ± 8,357 |

```
serialize SMALL (ns/op)
protobuf     ██████░░░░░░░░░░░░░░░░░░░░░░░░          511
avro         █████████░░░░░░░░░░░░░░░░░░░░░          794
pojo-hinted  ███████░░░░░░░░░░░░░░░░░░░░░░░          651
pojo-kryo    ██████████████████████████████        2,654
pojo-plain   ████████████████░░░░░░░░░░░░░░        1,438

serialize MEDIUM (ns/op)
protobuf     ███████████░░░░░░░░░░░░░░░░░░░        3,749
avro         █████████████░░░░░░░░░░░░░░░░░        4,205
pojo-hinted  █████████████████░░░░░░░░░░░░░        5,623
pojo-kryo    ██████████████████████████████       10,026
pojo-plain   ██████████████████████████░░░░        8,534

serialize LARGE (ns/op)
protobuf     █████████████░░░░░░░░░░░░░░░░░       57,510
avro         █████████████████░░░░░░░░░░░░░       73,799
pojo-hinted  ██████████████████████░░░░░░░░       96,239
pojo-kryo    █████████████████████████████░      129,784
pojo-plain   ██████████████████████████████      133,448
```

#### deserialize (new object each time) (ns/op, lower is better)

| variant | SMALL | MEDIUM | LARGE |
|---|---:|---:|---:|
| protobuf | 1,108 ± 76.6 | 6,656 ± 191 | 109,338 ± 10,130 |
| pojo-hinted | 1,466 ± 57.3 | 7,441 ± 381 | 143,929 ± 3,501 |
| avro | 1,927 ± 160 | 11,136 ± 707 | 181,504 ± 22,192 |
| pojo-kryo | 5,616 ± 214 | 21,408 ± 676 | 379,868 ± 8,983 |
| pojo-plain | 4,143 ± 91.2 | 20,057 ± 696 | 381,206 ± 10,658 |

```
deserialize SMALL (ns/op)
protobuf     ██████░░░░░░░░░░░░░░░░░░░░░░░░        1,108
pojo-hinted  ████████░░░░░░░░░░░░░░░░░░░░░░        1,466
avro         ██████████░░░░░░░░░░░░░░░░░░░░        1,927
pojo-kryo    ██████████████████████████████        5,616
pojo-plain   ██████████████████████░░░░░░░░        4,143

deserialize MEDIUM (ns/op)
protobuf     █████████░░░░░░░░░░░░░░░░░░░░░        6,656
pojo-hinted  ██████████░░░░░░░░░░░░░░░░░░░░        7,441
avro         ████████████████░░░░░░░░░░░░░░       11,136
pojo-kryo    ██████████████████████████████       21,408
pojo-plain   ████████████████████████████░░       20,057

deserialize LARGE (ns/op)
protobuf     █████████░░░░░░░░░░░░░░░░░░░░░      109,338
pojo-hinted  ███████████░░░░░░░░░░░░░░░░░░░      143,929
avro         ██████████████░░░░░░░░░░░░░░░░      181,504
pojo-kryo    ██████████████████████████████      379,868
pojo-plain   ██████████████████████████████      381,206
```

#### deserialize (reusing one object) (ns/op, lower is better)

| variant | SMALL | MEDIUM | LARGE |
|---|---:|---:|---:|
| protobuf | 1,084 ± 46.3 | 6,460 ± 316 | 107,832 ± 5,238 |
| pojo-hinted | 1,256 ± 45.4 | 7,210 ± 283 | 143,380 ± 3,723 |
| avro | 1,928 ± 75.6 | 9,900 ± 299 | 158,094 ± 7,277 |
| pojo-kryo | 5,681 ± 325 | 21,326 ± 820 | 379,524 ± 12,346 |
| pojo-plain | 3,848 ± 140 | 19,554 ± 450 | 387,863 ± 10,794 |

```
deserializeReuse SMALL (ns/op)
protobuf     ██████░░░░░░░░░░░░░░░░░░░░░░░░        1,084
pojo-hinted  ███████░░░░░░░░░░░░░░░░░░░░░░░        1,256
avro         ██████████░░░░░░░░░░░░░░░░░░░░        1,928
pojo-kryo    ██████████████████████████████        5,681
pojo-plain   ████████████████████░░░░░░░░░░        3,848

deserializeReuse MEDIUM (ns/op)
protobuf     █████████░░░░░░░░░░░░░░░░░░░░░        6,460
pojo-hinted  ██████████░░░░░░░░░░░░░░░░░░░░        7,210
avro         ██████████████░░░░░░░░░░░░░░░░        9,900
pojo-kryo    ██████████████████████████████       21,326
pojo-plain   ████████████████████████████░░       19,554

deserializeReuse LARGE (ns/op)
protobuf     ████████░░░░░░░░░░░░░░░░░░░░░░      107,832
pojo-hinted  ███████████░░░░░░░░░░░░░░░░░░░      143,380
avro         ████████████░░░░░░░░░░░░░░░░░░      158,094
pojo-kryo    █████████████████████████████░      379,524
pojo-plain   ██████████████████████████████      387,863
```

#### round trip (serialize + deserialize) (ns/op, lower is better)

| variant | SMALL | MEDIUM | LARGE |
|---|---:|---:|---:|
| protobuf | 1,632 ± 76.8 | 10,119 ± 361 | 159,396 ± 3,771 |
| pojo-hinted | 2,306 ± 139 | 13,332 ± 495 | 241,702 ± 6,945 |
| avro | 2,843 ± 110 | 15,488 ± 505 | 256,593 ± 10,800 |
| pojo-kryo | 7,719 ± 210 | 31,452 ± 1,378 | 513,691 ± 14,576 |
| pojo-plain | 6,031 ± 196 | 29,061 ± 1,156 | 522,010 ± 14,953 |

```
roundTrip SMALL (ns/op)
protobuf     ██████░░░░░░░░░░░░░░░░░░░░░░░░        1,632
pojo-hinted  █████████░░░░░░░░░░░░░░░░░░░░░        2,306
avro         ███████████░░░░░░░░░░░░░░░░░░░        2,843
pojo-kryo    ██████████████████████████████        7,719
pojo-plain   ███████████████████████░░░░░░░        6,031

roundTrip MEDIUM (ns/op)
protobuf     ██████████░░░░░░░░░░░░░░░░░░░░       10,119
pojo-hinted  █████████████░░░░░░░░░░░░░░░░░       13,332
avro         ███████████████░░░░░░░░░░░░░░░       15,488
pojo-kryo    ██████████████████████████████       31,452
pojo-plain   ████████████████████████████░░       29,061

roundTrip LARGE (ns/op)
protobuf     █████████░░░░░░░░░░░░░░░░░░░░░      159,396
pojo-hinted  ██████████████░░░░░░░░░░░░░░░░      241,702
avro         ███████████████░░░░░░░░░░░░░░░      256,593
pojo-kryo    ██████████████████████████████      513,691
pojo-plain   ██████████████████████████████      522,010
```

#### copy (ns/op, lower is better)

| variant | SMALL | MEDIUM | LARGE |
|---|---:|---:|---:|
| protobuf | 7.3 ± 0.2 | 4.8 ± 0.1 | 4.3 ± 0.2 |
| avro | 945 ± 41.5 | 5,071 ± 727 | 77,819 ± 2,041 |
| pojo-hinted | 795 ± 62.4 | 4,745 ± 204 | 77,939 ± 1,952 |
| pojo-plain | 1,727 ± 82.0 | 9,519 ± 767 | 200,134 ± 9,271 |
| pojo-kryo | 1,884 ± 117 | 10,245 ± 435 | 236,919 ± 13,381 |

```
copy SMALL (ns/op)
protobuf     █░░░░░░░░░░░░░░░░░░░░░░░░░░░░░          7.3
avro         ███████████████░░░░░░░░░░░░░░░          945
pojo-hinted  █████████████░░░░░░░░░░░░░░░░░          795
pojo-plain   ████████████████████████████░░        1,727
pojo-kryo    ██████████████████████████████        1,884

copy MEDIUM (ns/op)
protobuf     █░░░░░░░░░░░░░░░░░░░░░░░░░░░░░          4.8
avro         ███████████████░░░░░░░░░░░░░░░        5,071
pojo-hinted  ██████████████░░░░░░░░░░░░░░░░        4,745
pojo-plain   ████████████████████████████░░        9,519
pojo-kryo    ██████████████████████████████       10,245

copy LARGE (ns/op)
protobuf     █░░░░░░░░░░░░░░░░░░░░░░░░░░░░░          4.3
avro         ██████████░░░░░░░░░░░░░░░░░░░░       77,819
pojo-hinted  ██████████░░░░░░░░░░░░░░░░░░░░       77,939
pojo-plain   █████████████████████████░░░░░      200,134
pojo-kryo    ██████████████████████████████      236,919
```

#### allocation per round trip (B/op, lower is better)

| variant | SMALL | MEDIUM | LARGE |
|---|---:|---:|---:|
| protobuf | 2,422 | 13,039 | 248,582 |
| pojo-hinted | 1,502 | 8,152 | 157,113 |
| avro | 2,288 | 13,392 | 251,338 |
| pojo-kryo | 1,566 | 6,456 | 110,163 |
| pojo-plain | 1,726 | 6,616 | 110,323 |

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
| pojo-hinted | 4,959 | 79.5 ± 2.1 |
| pojo-plain | 5,867 | 106 ± 3.6 |
| pojo-kryo | 943 | 15.9 ± 0.3 |
| avro | 1,769 | 45.1 ± 2.3 |
| protobuf | 113 | 1.7 ± 0.0 |


<!-- results:end -->

## Flink job results

<!-- jobs:start -->
<!-- jobs:end -->

## Notes for Claude

- Independent Maven project, no parent pom. Java 11, Flink 1.18.1.
- `flink-protobuf-serde` comes from the local `~/.m2`; install it from the sibling directory first.
- Avro classes are generated from `src/main/resources/avro`, protobuf classes from `src/main/resources/proto` using a pinned `protoc.exe` downloaded from Maven Central (no local protoc needed); both land in `target/generated-sources`.
- The POJO `Order` hints `List`/`Map` fields with `@TypeInfo` factories; `SchemaSmokeTest` fails if any field resolves to `GenericTypeInfo` (Kryo).
- Five variants (pojo-hinted, pojo-plain, pojo-kryo, avro, protobuf); see PLAN.md. Only pojo-plain and pojo-kryo may use Kryo, the rest must work with generic types disabled.
