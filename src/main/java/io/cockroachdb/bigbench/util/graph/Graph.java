package io.cockroachdb.bigbench.util.graph;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface Graph<N, V> {
    boolean contains(N node);

    Set<N> nodes();

    Collection<Edge<N, V>> edges();

    Optional<V> edgeValue(N from, N to);

    Set<N> adjacentNodes(N node);

    List<N> topologicalSort(boolean reverse);
}
