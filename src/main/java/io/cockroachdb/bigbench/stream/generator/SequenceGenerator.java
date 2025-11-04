package io.cockroachdb.bigbench.stream.generator;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import io.cockroachdb.bigbench.model.Identity;

public class SequenceGenerator implements ValueGenerator<Long> {
    private final long startNumber;

    private final long stopNumber;

    private final AtomicLong nextNumber = new AtomicLong();

    private final int increment;

    public SequenceGenerator(Identity gen) {
        this.increment = Math.max(1, gen.getStep());

        if (Objects.nonNull(gen.getFrom())) {
            this.startNumber = gen.getFrom();
        } else {
            this.startNumber = 1;
        }

        if (Objects.nonNull(gen.getTo())) {
            this.stopNumber = gen.getTo();
        } else {
            this.stopNumber = Long.MAX_VALUE;
        }

        this.nextNumber.set(startNumber);
    }

    @Override
    public Long nextValue() {
        long next = nextNumber.get();
        if (nextNumber.addAndGet(increment) > stopNumber) {
            nextNumber.set(startNumber);
        }
        return next;
    }
}
