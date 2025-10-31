package io.cockroachdb.bigbench.expression;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

public class ExpressionLoadTest {
    @Test
    void parallelExpressions_performance() {
        DefaultExpressionRegistry registry = new DefaultExpressionRegistry();
        registry.addVariable("pi", Math.PI);
        registry.addVariable("r", 25);

        registry.addFunction(FunctionDef.builder()
                .withId("pow")
                .withArgs(List.of("arg1", "arg2"))
                .withFunction(args -> {
                    Number arg1 = (Number) args[0];
                    Number arg2 = (Number) args[1];
                    return Math.pow(arg1.intValue(), arg2.intValue());
                }).build());

        AtomicInteger c = new AtomicInteger();

        List<Runnable> tasks = new ArrayList<>();

        IntStream.rangeClosed(1, 30).forEach(value -> {
            tasks.add(() -> {
                IntStream.range(1, 50_000).forEach(r -> {
                    Expression.evaluate("pow(2,3)", BigDecimal.class, registry);
                    c.incrementAndGet();
                });
            });
        });

        Instant t1 = Instant.now();

        runConcurrentlyAndWait(tasks);

        Duration d = Duration.between(t1, Instant.now());
        System.out.printf("%,d (%,d/s) in %s\n", c.get(), c.get() / d.toSeconds(), d);
    }

    private static void runConcurrentlyAndWait(List<Runnable> tasks) {
        List<CompletableFuture<?>> allFutures = new ArrayList<>();
        tasks.forEach(runnable -> allFutures.add(CompletableFuture.runAsync(runnable)));
        CompletableFuture.allOf(allFutures.toArray(new CompletableFuture[] {})).join();
    }
}
