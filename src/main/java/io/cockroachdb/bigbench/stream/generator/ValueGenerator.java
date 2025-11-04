package io.cockroachdb.bigbench.stream.generator;

public interface ValueGenerator<T> {
    T nextValue();
}
