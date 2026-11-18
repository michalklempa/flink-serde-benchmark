# flink-serde-benchmark

Flink state serializer benchmark: POJO (`PojoSerializer`) vs Protobuf (`ProtoSerializer` from [
`flink-protobuf`](../flink-protobuf)) vs Avro (`AvroSerializer`) on the same logical data.

## Results

<!-- results:start -->
Mode `full`: 5 forks, 5 x 1 s warmup, 5 x 1 s measurement.
Java 11.0.29, Linux, 8 CPUs, run on 2026-11-17.

#### serialize (ns/op, lower is better)

```
serialize SMALL (ns/op)
protobuf     ███████░░░░░░░░░░░░░░░░░░░░░░░          852
pojo-hinted  ███████████░░░░░░░░░░░░░░░░░░░        1,260
avro         ███████████░░░░░░░░░░░░░░░░░░░        1,309
pojo-kryo    ██████████████████████████████        3,483
pojo-plain   █████████████████████████░░░░░        2,919

serialize MEDIUM (ns/op)
protobuf     ██████████████████░░░░░░░░░░░░        8,769
pojo-hinted  ███████████████████░░░░░░░░░░░        9,278
avro         ██████████████████████░░░░░░░░       10,700
pojo-kryo    ██████████████████████████████       14,622
pojo-plain   ██████████████████████████████       14,663

serialize LARGE (ns/op)
protobuf     ████████████████░░░░░░░░░░░░░░      132,007
pojo-hinted  ███████████████████░░░░░░░░░░░      162,386
avro         ███████████████████████░░░░░░░      190,989
pojo-kryo    ██████████████████████████████      252,589
pojo-plain   ██████████████████████████████      254,117
```

| variant     |        SMALL |         MEDIUM |           LARGE |
|-------------|-------------:|---------------:|----------------:|
| protobuf    |   852 ± 24.3 |    8,769 ± 231 | 132,007 ± 3,156 |
| pojo-hinted |  1,260 ± 350 |    9,278 ± 224 | 162,386 ± 2,729 |
| avro        | 1,309 ± 20.2 |   10,700 ± 367 | 190,989 ± 3,772 |
| pojo-kryo   | 3,483 ± 29.6 |   14,622 ± 223 | 252,589 ± 4,073 |
| pojo-plain  | 2,919 ± 19.5 | 14,663 ± 1,210 | 254,117 ± 7,414 |

#### deserialize (new object each time) (ns/op, lower is better)

```
deserialize SMALL (ns/op)
pojo-hinted  █████████░░░░░░░░░░░░░░░░░░░░░        1,797
protobuf     ███████░░░░░░░░░░░░░░░░░░░░░░░        1,539
avro         ████████████░░░░░░░░░░░░░░░░░░        2,434
pojo-kryo    ██████████████████████████████        6,194
pojo-plain   ██████████████████████░░░░░░░░        4,606

deserialize MEDIUM (ns/op)
pojo-hinted  ████████████░░░░░░░░░░░░░░░░░░       10,939
protobuf     ███████████████░░░░░░░░░░░░░░░       13,062
avro         ██████████████████░░░░░░░░░░░░       16,309
pojo-kryo    ██████████████████████████████       26,607
pojo-plain   █████████████████████████████░       25,381

deserialize LARGE (ns/op)
pojo-hinted  █████████████░░░░░░░░░░░░░░░░░      216,168
protobuf     ███████████████░░░░░░░░░░░░░░░      243,119
avro         ███████████████████░░░░░░░░░░░      323,517
pojo-kryo    █████████████████████████████░      492,257
pojo-plain   ██████████████████████████████      500,851
```

| variant     |        SMALL |         MEDIUM |            LARGE |
|-------------|-------------:|---------------:|-----------------:|
| pojo-hinted | 1,797 ± 33.6 |   10,939 ± 268 |  216,168 ± 3,415 |
| protobuf    | 1,539 ± 20.9 | 13,062 ± 1,269 |  243,119 ± 2,817 |
| avro        | 2,434 ± 63.6 |   16,309 ± 302 | 323,517 ± 11,132 |
| pojo-kryo   | 6,194 ± 59.3 |   26,607 ± 378 |  492,257 ± 5,709 |
| pojo-plain  | 4,606 ± 92.2 |   25,381 ± 339 |  500,851 ± 3,958 |

#### deserialize (reusing one object) (ns/op, lower is better)

```
deserializeReuse SMALL (ns/op)
pojo-hinted  ███████░░░░░░░░░░░░░░░░░░░░░░░        1,460
protobuf     ███████░░░░░░░░░░░░░░░░░░░░░░░        1,530
avro         ███████████░░░░░░░░░░░░░░░░░░░        2,326
pojo-kryo    ██████████████████████████████        6,235
pojo-plain   ████████████████████░░░░░░░░░░        4,235

deserializeReuse MEDIUM (ns/op)
pojo-hinted  ████████████░░░░░░░░░░░░░░░░░░       10,549
protobuf     ██████████████░░░░░░░░░░░░░░░░       12,742
avro         █████████████████░░░░░░░░░░░░░       15,296
pojo-kryo    ██████████████████████████████       26,628
pojo-plain   ████████████████████████████░░       24,918

deserializeReuse LARGE (ns/op)
pojo-hinted  █████████████░░░░░░░░░░░░░░░░░      214,888
protobuf     ███████████████░░░░░░░░░░░░░░░      249,311
avro         █████████████████░░░░░░░░░░░░░      286,978
pojo-kryo    █████████████████████████████░      494,907
pojo-plain   ██████████████████████████████      506,884
```

| variant     |        SMALL |       MEDIUM |            LARGE |
|-------------|-------------:|-------------:|-----------------:|
| pojo-hinted | 1,460 ± 29.0 | 10,549 ± 101 |  214,888 ± 3,064 |
| protobuf    | 1,530 ± 16.2 | 12,742 ± 274 |  249,311 ± 3,564 |
| avro        | 2,326 ± 45.5 | 15,296 ± 264 |  286,978 ± 2,718 |
| pojo-kryo   |  6,235 ± 140 | 26,628 ± 356 |  494,907 ± 9,937 |
| pojo-plain  | 4,235 ± 53.0 | 24,918 ± 299 | 506,884 ± 10,215 |

#### round trip (deserialize + serialize) (ns/op, lower is better)

| variant     |        SMALL |       MEDIUM |            LARGE |
|-------------|-------------:|-------------:|-----------------:|
| pojo-hinted | 2,706 ± 56.6 | 18,276 ± 357 |  374,000 ± 8,429 |
| protobuf    | 3,064 ± 45.8 | 23,341 ± 242 |  434,890 ± 5,132 |
| avro        |  3,935 ± 121 | 25,886 ± 387 |  497,334 ± 4,410 |
| pojo-kryo   |  9,796 ± 180 | 43,968 ± 509 |  809,326 ± 8,806 |
| pojo-plain  | 7,172 ± 62.4 | 41,256 ± 681 | 809,925 ± 12,185 |

```
roundTrip SMALL (ns/op)
pojo-hinted  ████████░░░░░░░░░░░░░░░░░░░░░░        2,706
protobuf     █████████░░░░░░░░░░░░░░░░░░░░░        3,064
avro         ████████████░░░░░░░░░░░░░░░░░░        3,935
pojo-kryo    ██████████████████████████████        9,796
pojo-plain   ██████████████████████░░░░░░░░        7,172

roundTrip MEDIUM (ns/op)
pojo-hinted  ████████████░░░░░░░░░░░░░░░░░░       18,276
protobuf     ████████████████░░░░░░░░░░░░░░       23,341
avro         ██████████████████░░░░░░░░░░░░       25,886
pojo-kryo    ██████████████████████████████       43,968
pojo-plain   ████████████████████████████░░       41,256

roundTrip LARGE (ns/op)
pojo-hinted  ██████████████░░░░░░░░░░░░░░░░      374,000
protobuf     ████████████████░░░░░░░░░░░░░░      434,890
avro         ██████████████████░░░░░░░░░░░░      497,334
pojo-kryo    ██████████████████████████████      809,326
pojo-plain   ██████████████████████████████      809,925
```

#### copy (ns/op, lower is better)

| variant     |        SMALL |       MEDIUM |           LARGE |
|-------------|-------------:|-------------:|----------------:|
| protobuf    |    7.4 ± 0.3 |    4.8 ± 0.1 |       4.3 ± 0.1 |
| avro        |   877 ± 22.8 |  5,544 ± 146 |  86,639 ± 1,325 |
| pojo-hinted |   925 ± 88.4 |  4,861 ± 130 |  88,929 ± 1,256 |
| pojo-plain  | 1,633 ± 50.1 |  9,640 ± 163 | 155,861 ± 2,582 |
| pojo-kryo   |  1,903 ± 151 | 10,832 ± 168 | 192,032 ± 4,329 |

```
copy SMALL (ns/op)
protobuf     █░░░░░░░░░░░░░░░░░░░░░░░░░░░░░          7.4
avro         ██████████████░░░░░░░░░░░░░░░░          877
pojo-hinted  ███████████████░░░░░░░░░░░░░░░          925
pojo-plain   ██████████████████████████░░░░        1,633
pojo-kryo    ██████████████████████████████        1,903

copy MEDIUM (ns/op)
protobuf     █░░░░░░░░░░░░░░░░░░░░░░░░░░░░░          4.8
avro         ███████████████░░░░░░░░░░░░░░░        5,544
pojo-hinted  █████████████░░░░░░░░░░░░░░░░░        4,861
pojo-plain   ███████████████████████████░░░        9,640
pojo-kryo    ██████████████████████████████       10,832

copy LARGE (ns/op)
protobuf     █░░░░░░░░░░░░░░░░░░░░░░░░░░░░░          4.3
avro         ██████████████░░░░░░░░░░░░░░░░       86,639
pojo-hinted  ██████████████░░░░░░░░░░░░░░░░       88,929
pojo-plain   ████████████████████████░░░░░░      155,861
pojo-kryo    ██████████████████████████████      192,032
```

#### allocation per round trip (B/op, lower is better)

| variant     | SMALL | MEDIUM |   LARGE |
|-------------|------:|-------:|--------:|
| pojo-hinted | 1,668 |  9,966 | 202,767 |
| protobuf    | 3,146 | 18,349 | 364,702 |
| avro        | 2,959 | 21,359 | 440,105 |
| pojo-kryo   | 1,736 |  8,274 | 155,821 |
| pojo-plain  | 1,892 |  8,431 | 155,978 |

#### serialized size (bytes per order, lower is better)

| variant     | SMALL | MEDIUM |  LARGE |
|-------------|------:|-------:|-------:|
| pojo-hinted |   239 |  1,972 | 47,191 |
| pojo-plain  |   350 |  2,050 | 45,539 |
| pojo-kryo   |   420 |  2,093 | 45,375 |
| avro        |   240 |  2,049 | 47,805 |
| protobuf    |   215 |  1,955 | 46,504 |

#### serializer snapshot (written into every checkpoint)

| variant     | snapshot bytes | write + read + compatibility check (µs/op) |
|-------------|---------------:|-------------------------------------------:|
| pojo-hinted |          4,959 |                                 76.3 ± 0.6 |
| pojo-plain  |          5,867 |                                  101 ± 0.8 |
| pojo-kryo   |            943 |                                 14.8 ± 0.1 |
| avro        |          1,769 |                                 38.7 ± 0.5 |
| protobuf    |            113 |                                  1.7 ± 0.0 |

<!-- results:end -->

## Flink job results

<!-- jobs:start -->
Mode `full`: parallelism 1, chaining off, one JVM per run, 3 repeat (s), median. Java 11.0.29, Linux, 8 CPUs, run on
2026-11-17.

#### Job A: operator to operator (records/s, higher is better)

| variant     |   SMALL | MEDIUM | LARGE |
|-------------|--------:|-------:|------:|
| protobuf    | 244,906 | 34,139 | 1,866 |
| pojo-hinted | 219,130 | 42,108 | 2,201 |
| avro        | 181,781 | 28,460 | 1,636 |
| pojo-plain  |  94,508 | 17,673 |   992 |
| pojo-kryo   |  75,008 | 17,102 |   998 |

```
job A SMALL (records/s)
protobuf     ██████████████████████████████      244,906
pojo-hinted  ███████████████████████████░░░      219,130
avro         ██████████████████████░░░░░░░░      181,781
pojo-plain   ████████████░░░░░░░░░░░░░░░░░░       94,508
pojo-kryo    █████████░░░░░░░░░░░░░░░░░░░░░       75,008

job A MEDIUM (records/s)
protobuf     ████████████████████████░░░░░░       34,139
pojo-hinted  ██████████████████████████████       42,108
avro         ████████████████████░░░░░░░░░░       28,460
pojo-plain   █████████████░░░░░░░░░░░░░░░░░       17,673
pojo-kryo    ████████████░░░░░░░░░░░░░░░░░░       17,102

job A LARGE (records/s)
protobuf     █████████████████████████░░░░░        1,866
pojo-hinted  ██████████████████████████████        2,201
avro         ██████████████████████░░░░░░░░        1,636
pojo-plain   ██████████████░░░░░░░░░░░░░░░░          992
pojo-kryo    ██████████████░░░░░░░░░░░░░░░░          998
```

#### Job B: keyed ValueState on RocksDB (records/s, higher is better)

| variant     |  SMALL | MEDIUM | LARGE |
|-------------|-------:|-------:|------:|
| protobuf    | 56,883 | 14,407 |   889 |
| avro        | 45,682 | 12,446 |   726 |
| pojo-hinted | 31,258 | 17,080 |   915 |
| pojo-kryo   | 21,258 |  7,832 |   489 |
| pojo-plain  | 17,921 |  8,080 |   482 |

```
job B SMALL (records/s)
protobuf     ██████████████████████████████       56,883
avro         ████████████████████████░░░░░░       45,682
pojo-hinted  ████████████████░░░░░░░░░░░░░░       31,258
pojo-kryo    ███████████░░░░░░░░░░░░░░░░░░░       21,258
pojo-plain   █████████░░░░░░░░░░░░░░░░░░░░░       17,921

job B MEDIUM (records/s)
protobuf     █████████████████████████░░░░░       14,407
avro         ██████████████████████░░░░░░░░       12,446
pojo-hinted  ██████████████████████████████       17,080
pojo-kryo    ██████████████░░░░░░░░░░░░░░░░        7,832
pojo-plain   ██████████████░░░░░░░░░░░░░░░░        8,080

job B LARGE (records/s)
protobuf     █████████████████████████████░          889
avro         ████████████████████████░░░░░░          726
pojo-hinted  ██████████████████████████████          915
pojo-kryo    ████████████████░░░░░░░░░░░░░░          489
pojo-plain   ████████████████░░░░░░░░░░░░░░          482
```

<!-- jobs:end -->

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

`ReportMain [quick|full] [readme-path]` runs every benchmark through the JMH `Runner` API with the GC profiler and
rewrites the block between the `results:start` / `results:end` markers in the README: markdown tables plus ASCII bar
charts (plain block characters in a code block, readable anywhere). Nothing but the JDK and this jar is needed.

### Run durations

`SerdeBenchmark` has 5 methods x 5 variants x 3 profiles = 75 benchmarks, `SnapshotBenchmark` has 5, so 80 in total.
Duration is roughly `benchmarks x forks x (warmup + measurement iterations x time + ~1 s fork startup)`.

| Mode    | Settings                                     | Per benchmark | Total    |
|---------|----------------------------------------------|---------------|----------|
| `quick` | 1 fork, 1 x 1 s warmup, 2 x 1 s measurement  | ~4 s          | ~5.5 min |
| `full`  | 5 forks, 5 x 1 s warmup, 5 x 1 s measurement | ~55 s         | ~75 min  |

The `quick` run is for a first comparison and sanity check; one fork and two 1 s iterations give noisy scores, so use
`full` for numbers you publish. The durations are estimates from the formula, not timed runs.

Raw JMH CLI is still available for ad-hoc runs, for example:
`java -jar target/benchmarks.jar SerdeBenchmark.serialize -p profile=SMALL -f 1 -wi 1 -i 2 -w 1 -r 1 -prof gc`.

## Flink jobs

Two jobs run on a mini cluster in a single JVM with parallelism 1, max parallelism 128 and operator chaining off, so
every hop goes through the serializer:

- Job A: `source -> pass-through map -> sink` (record serde between operators).
- Job B: `source -> keyBy -> KeyedProcessFunction with ValueState<Order> (read, then update) -> sink`, on the RocksDB
  state backend. Keys are skewed (`K * u^3` for uniform `u`), so low keys are hot. Job B also adds `keyBy` and a
  `Tuple2` wrapper, so the two jobs are not subtracted from each other.

Throughput is measured in the sink: it stores `System.nanoTime()` at record W (10 percent of N) and at record N into
accumulators, and throughput is `(N - W) / elapsed`, which excludes job startup and warmup. Every run is a separate JVM
started by the driver; the median over the repeats is reported.

```bash
java -cp target/benchmarks.jar com.michalklempa.flink.serde.benchmark.job.JobBenchmarkMain quick
java -cp target/benchmarks.jar com.michalklempa.flink.serde.benchmark.job.JobBenchmarkMain full
```

| Mode    | Records N (small / medium / large) | Keys K                   | Repeats | Runs | Estimated time |
|---------|------------------------------------|--------------------------|---------|------|----------------|
| `quick` | 300,000 / 75,000 / 4,500           | 100,000 / 10,000 / 1,000 | 1       | 30   | about 5 min    |
| `full`  | 2,400,000 / 600,000 / 35,000       | 100,000 / 10,000 / 1,000 | 3       | 90   | about 60 min   |

Sizing: a run costs about 5.5 s of JVM and mini cluster startup plus `N / throughput`. Measured throughput on the dev
machine (records/s, fastest and slowest variant): Job A small 239k / 103k, medium 44k / 21k, large 2.3k / 1.2k; Job B
small 74k / 31k, medium 19k / 8k, large 970 / 608. N is chosen so each profile takes about the same share of time, which
gives about 20 minutes per repeat of the full matrix (about 5 minutes for `quick`). The totals are estimates from these
rates, not a timed run; the Kryo variants take about twice as long as the others.

A single run:
`java -cp target/benchmarks.jar com.michalklempa.flink.serde.benchmark.job.JobRunner --job=B --variant=protobuf --profile=MEDIUM --records=50000 --keys=10000`.

## Methodology and caveats

- Only the `TypeSerializer` is measured, in memory, with a reused `DataOutputSerializer` / `DataInputDeserializer`. No
  state backend, network or checkpointing is involved.
- Each benchmark cycles through a pool of pre-generated orders (4096 small, 1024 medium, 256 large) so the JIT cannot
  specialise on one object and the working set is larger than L1/L2 cache. Generation and mapping are outside the timed
  code.
- `deserialize` creates a new object every call; `deserializeReuse` calls `deserialize(reuse, source)` with one object,
  as Flink does when object reuse is enabled. Protobuf messages and Kryo ignore the reuse object, so for them both
  numbers match.
- Avro uses generated specific records: classes generated from `order.avsc` by `avro-maven-plugin` (`SpecificRecord`,
  strings as `java.lang.String`), wrapped by `AvroTypeInfo` and serialized by Flink's `AvroSerializer`. Generic records
  and reflect-based POJOs are not benchmarked. The serializer snapshot carries the full Avro schema, which is why Avro's
  snapshot is larger than protobuf's.
- `pojo-plain` and `pojo-kryo` use Flink's default Kryo with no class registration, which is what an unaware developer
  gets; a tuned Kryo setup would be faster and smaller. All other variants run with `disableGenericTypes()`.
- Protobuf is proto3, which does not write default values. `vip` (false half of the time) and `status` (`NEW` a fifth of
  the time) are therefore sometimes omitted; every other field is practically never zero or empty.
- Generated data: strings vary in length around the profile's value and are about 16 percent non-ASCII; `customerId` is
  spread over all magnitudes so neither varint (Avro, protobuf) nor fixed-width (POJO) encoding is favoured; payload
  bytes are random and incompressible. The three profiles change all dimensions at once, so a difference between
  profiles cannot be attributed to one of them.
- `roundTrip` deserializes and then serializes the fresh object, as an operator hop does. `serialize` alone
  re-serializes pooled objects; protobuf messages cache their serialized size, so that number flatters protobuf.
- `ProtoSerializer` comes from the author's own `flink-protobuf` library. It serializes through `toByteArray()` (one
  extra copy) and its `copy` returns the immutable message as is, so the protobuf `copy` score is not a deep copy.
- Each JMH fork runs one variant, so serializer call sites are monomorphic and fully inlined. A real job mixes
  serializer types; absolute numbers here are optimistic for every variant.
- JMH forks and job JVMs run with a fixed 2 GB heap; JMH forks use ParallelGC.
- Not covered: schema evolution, nullable or optional fields, checkpoint and restore, tuned Kryo, `Row`/`RowData`, state
  larger than memory. One machine, one JDK, Flink 1.18.1.

## Notes for Claude

- Independent Maven project, no parent pom. Java 11, Flink 1.18.1.
- `flink-protobuf-serde` comes from the local `~/.m2`; install it from the sibling directory first.
- Avro classes are generated from `src/main/resources/avro`, protobuf classes from `src/main/resources/proto` using a
  pinned `protoc.exe` downloaded from Maven Central (no local protoc needed); both land in `target/generated-sources`.
- The POJO `Order` hints `List`/`Map` fields with `@TypeInfo` factories; `SchemaSmokeTest` fails if any field resolves
  to `GenericTypeInfo` (Kryo).
- Five variants (pojo-hinted, pojo-plain, pojo-kryo, avro, protobuf); see PLAN.md. Only pojo-plain and pojo-kryo may use
  Kryo, the rest must work with generic types disabled.

See [PLAN.md](PLAN.md) for status.
