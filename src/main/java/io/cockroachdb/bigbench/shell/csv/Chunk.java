package io.cockroachdb.bigbench.shell.csv;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public final class Chunk<T> implements Iterable<T> {
    @SafeVarargs
    public static <T> Chunk<T> of(T... items) {
        return new Chunk<>(items);
    }

    private final List<T> items;

    @SafeVarargs
    public Chunk(T... items) {
        this(Arrays.asList(items));
    }

    public Chunk(List<? extends T> items) {
        this.items = new ArrayList<>(items);
    }

    public void add(T item) {
        items.add(item);
    }

    public void addAll(List<T> items) {
        this.items.addAll(items);
    }

    public int size() {
        return items.size();
    }

    public void clear() {
        items.clear();
    }

    public List<T> getItems() {
        return List.copyOf(items);
    }

    @Override
    public Iterator<T> iterator() {
        return items.iterator();
    }
}
