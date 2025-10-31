package io.cockroachdb.bigbench.util;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

public class CartesianTest {
    @Test
    public void testProducts() {
        List<List<?>> cols = new ArrayList<>();

        cols.add(createInts(20));
        cols.add(createStrings(5));
        cols.add(List.of('X', 'Y', 'Z'));

        List<List<?>> cartesianProduct = getCartesianProduct(cols);
//        List<List<?>> cartesianProduct = getCartesianProductIterative(sets);
//        Stream<List<?>> cartesianProduct = cartesianProduct(sets, 0);

        cartesianProduct.forEach(objects -> {
            System.out.println(objects);
        });

        System.out.println(cartesianProduct.size());

    }

    private static List<Integer> createInts(int size) {
        List<Integer> list = new ArrayList<>();
        IntStream.rangeClosed(1,size).forEach(value ->  {
            list.add(value);
        });
        return list;
    }

    private static List<String> createStrings(int size) {
        List<String> list = new ArrayList<>();
        IntStream.rangeClosed(1,size).forEach(value ->  {
            list.add(RandomData.randomFirstName());
        });
        return list;
    }

    public static List<List<?>> getCartesianProductUsingStreams(List<List<?>> sets) {
        return cartesianProduct(sets,0)
                .collect(Collectors.toList());
    }

    public static Stream<List<?>> cartesianProduct(List<List<?>> sets, int index) {
        if (index == sets.size()) {
            return Stream.of(List.of());
        }

        List<?> currentSet = sets.get(index);

        return currentSet.stream().flatMap(element -> cartesianProduct(sets, index + 1)
                .map(list -> {
                    List<Object> newList = new ArrayList<>(list);
                    newList.add(0, element);
                    return newList;
                }));
    }

    public static List<List<?>> getCartesianProduct(List<List<?>> sets) {
        List<List<?>> result = new ArrayList<>();
        getCartesianProductHelper(sets, 0, new ArrayList<>(), result);
        return result;
    }

    private static void getCartesianProductHelper(List<List<?>> sets,
                                                  int index,
                                                  List<Object> current,
                                                  List<List<?>> result) {
        if (index == sets.size()) {
            result.add(new ArrayList<>(current));
        } else {
            List<?> currentSet = sets.get(index);

            for (Object element: currentSet) {
                current.add(element);
                getCartesianProductHelper(sets, index + 1, current, result);
                current.remove(current.size() - 1);
            }
        }
    }
}
