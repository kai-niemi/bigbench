package io.cockroachdb.bigbench.shell.support;

@FunctionalInterface
public interface ValueProvider<T> {
    Object getValue(T object, int column);
}
