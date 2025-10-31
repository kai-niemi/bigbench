package io.cockroachdb.bigbench.generator;

public interface ValueGenerator<T> {
    T nextValue();
}
