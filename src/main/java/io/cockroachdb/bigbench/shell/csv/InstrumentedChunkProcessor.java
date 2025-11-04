package io.cockroachdb.bigbench.shell.csv;

import java.util.List;
import java.util.Objects;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

public class InstrumentedChunkProcessor<T> implements ChunkProcessor<T> {
    private final ChunkProcessor<T> delegate;

    private final Timer processingTime;

    public InstrumentedChunkProcessor(ChunkProcessor<T> delegate, MeterRegistry meterRegistry) {
        Objects.requireNonNull(delegate, "delegate is null");
        Objects.requireNonNull(meterRegistry, "meterRegistry is null");

        this.delegate = delegate;

        this.processingTime = Timer
                .builder("bigbench.chunk.processing.time")
                .description("Chunk processing time")
                .publishPercentiles(.90, .95, .99, .999)
                .publishPercentileHistogram()
                .percentilePrecision(2)
                .register(meterRegistry);
    }

    public Timer getProcessingTime() {
        return processingTime;
    }

    @Override
    public void processHeader(List<String> columns) throws ProcessorException {
        delegate.processHeader(columns);
    }

    @Override
    public int processChunk(Chunk<? extends T> chunk) throws ProcessorException {
        return processingTime.record(() -> delegate.processChunk(chunk));
    }
}
