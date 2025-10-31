package io.cockroachdb.bigbench.util;

import org.junit.jupiter.api.Test;

public class RandomDataTest {
    @Test
    public void randomJson() {
        System.out.println(RandomData.randomJson(2, 2));
    }
}
