package io.cockroachdb.bigbench.util.graph;

import java.util.Objects;

public class Edge<T, V> {
    private final T start;

    private final T end;

    private V value;

    protected Edge(T start, T end, V value) {
        if (start == null) {
            throw new IllegalArgumentException("start is null");
        }
        if (end == null) {
            throw new IllegalArgumentException("end is null");
        }
        if (start == end) { // by ref
            throw new IllegalArgumentException("start == end");
        }
        this.start = start;
        this.end = end;
        this.value = value;
    }

    public T getEnd() {
        return end;
    }

    public T getStart() {
        return start;
    }

    public V getValue() {
        return value;
    }

    public Edge<T, V> setValue(V value) {
        this.value = value;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Edge)) {
            return false;
        }
        Edge<?, ?> edge = (Edge<?, ?>) o;
        return start.equals(edge.start) && end.equals(edge.end);
    }

    @Override
    public int hashCode() {
        return Objects.hash(start, end);
    }

    @Override
    public String toString() {
        return String.format("%s->%s (%s)", start, end, value);
    }
}