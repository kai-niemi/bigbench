package io.cockroachdb.bigbench.generator;

import java.util.UUID;

public class UUIDGenerator implements ValueGenerator<UUID> {
    @Override
    public UUID nextValue() {
        return UUID.randomUUID();
    }
}
