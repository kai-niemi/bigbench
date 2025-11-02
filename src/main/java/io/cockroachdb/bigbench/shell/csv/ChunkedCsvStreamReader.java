package io.cockroachdb.bigbench.shell.csv;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.cockroachdb.bigbench.util.AsciiArt;

public class ChunkedCsvStreamReader {
    public static final int QUEUE_SIZE = 1024;

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final int chunkSize;

    private final String delimiter;

    public ChunkedCsvStreamReader(int chunkSize, String delimiter) {
        this.chunkSize = chunkSize;
        this.delimiter = delimiter;
    }


    public void readInputStream(InputStream inputStream, ChunkProcessor<List<String>> chunkProcessor) {
        // Used bounded blocking queue to conserve memory
        final BlockingQueue<String> blockingQueue = new LinkedBlockingQueue<>(Math.max(QUEUE_SIZE, chunkSize));

        try (LineNumberReader lineNumberReader = new LineNumberReader(
                new BufferedReader(new InputStreamReader(inputStream)))) {
            chunkProcessor.processHeader(parseLine(lineNumberReader.readLine(), delimiter));

            // Start stream supplier / reader
            final CompletableFuture<Integer> supplierFuture = CompletableFuture.supplyAsync(() ->
                    supplyQueue(blockingQueue, lineNumberReader));

            // Start stream consumer
            final CompletableFuture<Integer> consumerFuture = CompletableFuture.supplyAsync(() ->
                    drainQueue(blockingQueue, chunkSize, chunkProcessor));

            final Instant startTime = Instant.now();

            CompletableFuture.allOf(consumerFuture, supplierFuture).join();

            final int rowsProduced = supplierFuture.get();
            final int rowsConsumed = consumerFuture.get();

            final Duration duration = Duration.between(startTime, Instant.now());

            AsciiArt.tock();

            logger.info("""
                    Finished reading input stream (%s)
                    Produced chunks: %,d
                      Produced rows: %,d
                      Consumed rows: %,d
                           Duration: %s
                           Rows/sec: %.1f/s""".formatted(
                    // Adjust +1 for header
                    rowsProduced != (rowsConsumed + 1) ? AsciiArt.flipTableRoughly() : AsciiArt.shrug(),
                    rowsProduced / chunkSize,
                    rowsProduced,
                    rowsConsumed,
                    duration,
                    rowsConsumed / Math.max(1, duration.toSeconds() + 0f)
            ));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ProcessorException(e);
        } catch (ExecutionException e) {
            throw new ProcessorException(e.getCause());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private List<String> parseLine(String line, String delimiter) {
        return Stream.of(line.split(delimiter))
                .map(String::trim)
                .toList();
    }

    private Integer supplyQueue(BlockingQueue<String> blockingQueue,
                                LineNumberReader lineNumberReader) {
        try {
            String line;
            while ((line = lineNumberReader.readLine()) != null) {
                blockingQueue.put(line);
            }

            // Poison the queue to signal consumer
            logger.debug("End of stream at row %d - injecting poison pill"
                    .formatted(lineNumberReader.getLineNumber()));
            blockingQueue.put("");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ProcessorException(e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return lineNumberReader.getLineNumber();
    }

    private Integer drainQueue(BlockingQueue<String> blockingQueue,
                               int batchSize,
                               ChunkProcessor<List<String>> chunkProcessor) {
        final AtomicInteger totalRows = new AtomicInteger();

        final Chunk<List<String>> chunk = Chunk.of();

        try {
            for (; ; ) {
                String row = blockingQueue.take();
                if (row.isEmpty()) {
                    logger.debug("Poising pill found - bugging out");
                    break;
                }

                chunk.add(parseLine(row, delimiter));

                if ((chunk.size() % batchSize) == 0) {
                    process(chunkProcessor, chunk, totalRows);
                    chunk.clear();
                }
            }

            process(chunkProcessor, chunk, totalRows);
            return totalRows.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ProcessorException(e);
        }
    }

    private void process(ChunkProcessor<List<String>> chunkProcessor, Chunk<List<String>> chunk,
                         AtomicInteger totalRows) {
        int rows = chunkProcessor.processChunk(chunk);
        totalRows.addAndGet(rows);
        AsciiArt.tick("Processing", totalRows.get());
    }
}
