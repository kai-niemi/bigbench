package io.cockroachdb.bigbench.stream.generator;

import java.time.temporal.Temporal;

import javax.sql.DataSource;

import org.springframework.util.StringUtils;

import io.cockroachdb.bigbench.model.Column;
import io.cockroachdb.bigbench.model.Identity;
import io.cockroachdb.bigbench.model.Range;
import io.cockroachdb.bigbench.model.ValueSet;
import io.cockroachdb.bigbench.expression.ExpressionRegistry;
import io.cockroachdb.bigbench.util.RandomData;

public abstract class ValueGenerators {
    private ValueGenerators() {
    }

    public static ValueGenerator<?> createValueGenerator(Column column,
                                                         DataSource dataSource,
                                                         ExpressionRegistry expressionRegistry) {
        if (column.getRange() != null) {
            return ValueGenerators.createRangeGenerator(column.getRange());
        }

        Identity gen = column.getIdentity();
        if (gen != null) {
            return ValueGenerators.createIdGenerator(gen, dataSource);
        }

        String constant = column.getConstant();
        if (StringUtils.hasLength(constant)) {
            return ValueGenerators.createConstantGenerator(column);
        }

        if (StringUtils.hasLength(column.getExpression())) {
            return ValueGenerators.createExpressionGenerator(column, expressionRegistry);
        }

        ValueSet<?> set = column.getValueSet();
        if (set != null) {
            return ValueGenerators.createValueSetGenerator(set);
        }

        throw new IllegalStateException("No column value generator found for " + column);
    }


    public static ValueGenerator<? extends Temporal> createRangeGenerator(Range range) {
        return switch (range.getType()) {
            case date -> new LocalDateRangeGenerator(range);
            case time -> new LocalTimeRangeGenerator(range);
            case datetime -> new LocalDateTimeGenerator(range);
        };
    }

    public static ValueGenerator<?> createIdGenerator(Identity gen, DataSource dataSource) {
        return switch (gen.getType()) {
            case uuid -> new UUIDGenerator();
            case sequence -> new SequenceGenerator(gen);
            case database_sequence -> new DatabaseSequenceGenerator(dataSource, gen);
            case ordered, unordered -> new RowIdGenerator(dataSource, gen);
        };
    }

    public static ValueGenerator<?> createConstantGenerator(Column column) {
        return column::getConstant;
    }

    public static ValueGenerator<?> createExpressionGenerator(Column column, ExpressionRegistry registry) {
        return new ExpressionGenerator(column, registry);
    }

    public static ValueGenerator<?> createValueSetGenerator(ValueSet<?> valueSet) {
        return () -> {
            if (valueSet.getWeights().isEmpty()) {
                return RandomData.selectRandom(valueSet.getValues());
            }
            return RandomData.selectRandomWeighted(valueSet.getValues(), valueSet.getWeights());
        };
    }
}
