package io.cockroachdb.bigbench.util.graph;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DirectedAcyclicGraphTest {
    @Test
    public void givenDirectedGraph_whenTopologicalSort_thenPrintValuesSorted() {
        DirectedAcyclicGraph<String, String> directedAcyclicGraph = new DirectedAcyclicGraph<>();

        directedAcyclicGraph.addEdge("order_item", "order", "fk_order_item_order");
        directedAcyclicGraph.addEdge("order_item", "product", "fk_order_item_product");
        directedAcyclicGraph.addEdge("order", "customer", "fk_order_product");
        directedAcyclicGraph.addEdge("product", "category", "fk_product_category");

        Assertions.assertEquals(Arrays.asList("order_item", "order", "customer", "product", "category"),
                directedAcyclicGraph.topologicalSort(false));
        Assertions.assertEquals(Arrays.asList("category","product","customer", "order", "order_item"),
                directedAcyclicGraph.topologicalSort(true));

        directedAcyclicGraph.edges();
        System.out.println(directedAcyclicGraph);
    }

    @Test
    public void givenDirectedGraph_whenTopologicalSort_thenPrintValuesSortedAgain() {
        DirectedAcyclicGraph<String, Integer> directedAcyclicGraph = new DirectedAcyclicGraph<>();

        directedAcyclicGraph.addNode("A");
        directedAcyclicGraph.addNode("B");
        directedAcyclicGraph.addNode("C");
        directedAcyclicGraph.addNode("D");
        directedAcyclicGraph.addNode("E");

        directedAcyclicGraph.addEdge("A", "B", 1);
        directedAcyclicGraph.addEdge("A", "C");
        directedAcyclicGraph.addEdge("A", "D", 2);
        directedAcyclicGraph.addEdge("B", "D");
        directedAcyclicGraph.addEdge("C", "E", 3);
        directedAcyclicGraph.addEdge("D", "E");

        List<String> result = directedAcyclicGraph.topologicalSort(false);
        Assertions.assertEquals(Arrays.asList("A", "C", "B", "D", "E"), result);

        System.out.println(directedAcyclicGraph);
    }

    @Test
    public void givenCyclicGraph_whenTopologicalSort_thenThrowException() {
        DirectedAcyclicGraph<String, Double> directedAcyclicGraph = new DirectedAcyclicGraph<>();

        directedAcyclicGraph.addNode("A");
        directedAcyclicGraph.addNode("B");
        directedAcyclicGraph.addNode("C");
        directedAcyclicGraph.addNode("D");
        directedAcyclicGraph.addNode("E");

        directedAcyclicGraph.addEdge("A", "B");
        directedAcyclicGraph.addEdge("A", "C");
        directedAcyclicGraph.addEdge("A", "D");
        directedAcyclicGraph.addEdge("B", "D");
        directedAcyclicGraph.addEdge("C", "E");
        directedAcyclicGraph.addEdge("D", "E");

        directedAcyclicGraph.addEdge("E", "A"); // cycle

        System.out.println(directedAcyclicGraph);

        Exception exception = Assertions.assertThrows(IllegalStateException.class, () -> directedAcyclicGraph.topologicalSort(false));

        Assertions.assertEquals("Cycle detected in [E->A] visited [A, B, D, E]", exception.getMessage());
    }
}
