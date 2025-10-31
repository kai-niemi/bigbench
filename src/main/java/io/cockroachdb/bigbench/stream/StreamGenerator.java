package io.cockroachdb.bigbench.stream;

import java.io.OutputStream;

@FunctionalInterface
public interface StreamGenerator {
    void streamTo(OutputStream outputStream);
}
