package io.cockroachdb.bigbench.shell.csv;

import java.util.List;

public interface ChunkProcessor<T> {
    void processHeader(List<String> columns) throws ProcessorException;

    int processChunk(Chunk<? extends T> chunk) throws ProcessorException;
}
