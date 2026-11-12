package com.michalklempa.flink.serde.benchmark.job;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class TimingSink<T> extends RichSinkFunction<T> {
    public static final String START = "startNanos";
    public static final String END = "endNanos";
    public static final String COUNT = "count";

    private final long warmup;
    private final long records;
    private transient long seen;
    private transient LongCounter start;
    private transient LongCounter end;
    private transient LongCounter count;

    public TimingSink(long warmup, long records) {
        this.warmup = warmup;
        this.records = records;
    }

    @Override
    public void open(Configuration parameters) {
        start = new LongCounter();
        end = new LongCounter();
        count = new LongCounter();
        getRuntimeContext().addAccumulator(START, start);
        getRuntimeContext().addAccumulator(END, end);
        getRuntimeContext().addAccumulator(COUNT, count);
    }

    @Override
    public void invoke(T value, Context context) {
        seen++;
        if (seen == warmup) {
            start.add(System.nanoTime());
        }
        if (seen == records) {
            end.add(System.nanoTime());
        }
    }

    @Override
    public void close() {
        count.add(seen);
    }
}
