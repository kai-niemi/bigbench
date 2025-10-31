package io.cockroachdb.bigbench.util.graph;

import org.springframework.data.util.Pair;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Basic implementation for an undirected graph or directed acyclic graph (no cycles) for
 * resolving dependency ordering through topology sorting using recursive
 * DFS traversal.
 *
 * @param <N> the node type
 * @param <V> the edge value type
 */
public class DirectedAcyclicGraph<N, V> implements Iterable<N>, Graph<N, V> {
    private final Set<N> nodes = new HashSet<>();

    private final Map<Pair<N, N>, Edge<N, V>> edges = new HashMap<>();

    public DirectedAcyclicGraph<N, V> addNode(N node) {
        this.nodes.add(node);
        return this;
    }

    public Edge<N, V> addEdge(N start, N end) {
        return addEdge(start, end, null);
    }

    public Edge<N, V> addEdge(N start, N end, V value) {
        if (!nodes.contains(start)) {
            this.nodes.add(start);
        }
        if (!nodes.contains(end)) {
            this.nodes.add(end);
        }
        if (Objects.equals(start, end)) {
            throw new IllegalStateException("Self-reference: "
                    + start + " == " + end);
        }
        Edge<N, V> edge = new Edge<>(start, end, value);
        edges.put(Pair.of(start, end), edge);
        return edge;
    }

    @Override
    public boolean contains(N node) {
        return nodes.contains(node);
    }

    @Override
    public Optional<V> edgeValue(N from, N to) {
        return edge(from, to).map(Edge::getValue);
    }

    public Optional<Edge<N, V>> edge(N start, N target) {
        return Optional.ofNullable(edges.get(Pair.of(start, target)));
    }

    @Override
    public Set<N> adjacentNodes(N node) {
        return edges.values()
                .stream()
                .filter(edge -> edge.getStart().equals(node))
                .map(Edge::getEnd)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<N> nodes() {
        return Collections.unmodifiableSet(nodes);
    }

    @Override
    public Collection<Edge<N, V>> edges() {
        return Collections.unmodifiableCollection(edges.values());
    }

    @Override
    public Iterator<N> iterator() {
        return nodes().iterator();
    }

    /**
     * Performs a topological sort on the graph where it's expected to have
     * directed edges with no cycles (DAG). If a cycle is detected
     * an exception is thrown (ex: a->b, b->a).
     *
     * @return topologically sorted list of nodes
     * @throws IllegalStateException if the graph contains at least one cycle
     */
    @Override
    public List<N> topologicalSort(boolean reverse) {
        Set<N> visited = new HashSet<>();
        Deque<N> stack = new ArrayDeque<>();
        Set<N> trail = new HashSet<>();

        nodes().forEach(node -> recursiveTopologicalSort(node, visited, stack, trail));

        List<N> list = new ArrayList<>(stack);
        if (reverse) {
            Collections.reverse(list);
        }

        return list;
    }

    private void recursiveTopologicalSort(N node, Set<N> visited, Deque<N> stack, Set<N> trail) {
        if (!visited.contains(node)) {
            visited.add(node);
            trail.add(node);
            for (N neighbor : adjacentNodes(node)) {
                if (trail.contains(neighbor)) {
                    throw new IllegalStateException(
                            "Cycle detected in [" + node.toString() + "->" + neighbor.toString() + "] visited "
                                    + visited);
                }
                if (!visited.contains(neighbor)) {
                    recursiveTopologicalSort(neighbor, visited, stack, trail);
                }
            }
            stack.push(node);
            trail.remove(node);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        edges().forEach(e -> {
            if (!sb.isEmpty()) {
                sb.append(",\n");
            }
            sb.append("    ");
            sb.append(e);
        });
        return "Graph {" +
                "\n  nodes = " + nodes +
                ",\n  edges = \n" + sb +
                "\n}";
    }
}