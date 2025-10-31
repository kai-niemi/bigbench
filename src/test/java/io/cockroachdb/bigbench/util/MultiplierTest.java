package io.cockroachdb.bigbench.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MultiplierTest {
    @Test
    public void whenUsingDecimalMultipliers_thenReturnMultipliedValues() {
        assertEquals(15.0, Multiplier.parseDouble("15"));
        assertEquals(.15, Multiplier.parseDouble(".15"));
        assertEquals(-.15, Multiplier.parseDouble("-.15"));
        assertEquals(.15, Multiplier.parseDouble("+.15"));
        assertEquals(1.15, Multiplier.parseDouble("+1.15"));
        assertEquals(-1.15, Multiplier.parseDouble("-1.15"));
        assertEquals(15.5 * 1_000, Multiplier.parseDouble("15.5K"));
        assertEquals(15.5 * 1_000_000, Multiplier.parseDouble("15.5M"));
        assertEquals(0.5 * 1_000_000, Multiplier.parseDouble("0.5M"));
        assertEquals(100 * 1_000_000, Multiplier.parseDouble("100M"));
        assertEquals(5 * 1_000_000_000.0, Multiplier.parseDouble("5G"));

        Assertions.assertThrows(NumberFormatException.class, () -> Multiplier.parseDouble("100MM"));
        Assertions.assertThrows(NumberFormatException.class, () -> Multiplier.parseDouble("100B"));
        Assertions.assertThrows(NumberFormatException.class, () -> Multiplier.parseDouble("M"));
    }

    @Test
    public void whenUsingIntegerMultipliers_thenReturnMultipliedValues() {
        assertEquals(15, Multiplier.parseLong("15"));
        assertEquals(-15, Multiplier.parseLong("-15"));
        assertEquals(155 * 1_000, Multiplier.parseLong("155K"));
        assertEquals(155 * 1_000_000, Multiplier.parseLong("155M"));
        assertEquals(5 * 1_000_000, Multiplier.parseLong("5M"));
        assertEquals(5 * 100_000, Multiplier.parseLong("500K"));
        assertEquals(100 * 1_000_000, Multiplier.parseLong("100M"));

        Assertions.assertThrows(NumberFormatException.class,
                () -> Multiplier.parseLong("100MM"));
        Assertions.assertThrows(NumberFormatException.class, () -> Multiplier.parseLong("100B"));
        Assertions.assertThrows(NumberFormatException.class, () -> Multiplier.parseLong("M"));
    }
}
