package io.cockroachdb.bigbench.stream;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import javax.sql.DataSource;

import io.cockroachdb.bigbench.expression.ExpressionRegistry;
import io.cockroachdb.bigbench.expression.ExpressionRegistryBuilder;
import io.cockroachdb.bigbench.expression.FunctionDef;
import io.cockroachdb.bigbench.model.Column;
import io.cockroachdb.bigbench.stream.generator.ValueGenerator;
import io.cockroachdb.bigbench.stream.generator.ValueGenerators;

public abstract class AbstractStreamGeneratorSupport implements StreamGenerator {
    protected static final Predicate<Column> VISIBLE_COLUMN_PREDICATE
            = column -> (column.isHidden() == null || !column.isHidden());

    protected Map<Column, ValueGenerator<?>> columnGenerators(DataSource dataSource,
                                                              List<Column> columns,
                                                              AtomicInteger currentRow) {
        return createColumnGenerators(dataSource, columns, List.of(FunctionDef.builder()
                .withCategory("other")
                .withId("rowNumber")
                .withDescription("Returns current row number.")
                .withReturnValue(Integer.class)
                .withFunction(args -> currentRow.get())
                .build()));

    }

    protected Map<Column, ValueGenerator<?>> createColumnGenerators(DataSource dataSource,
                                                                    List<Column> columns,
                                                                    List<FunctionDef> functionDefs) {
        ExpressionRegistry registry = ExpressionRegistryBuilder.build(dataSource);
        functionDefs.forEach(registry::addFunction);

        Map<Column, ValueGenerator<?>> generatorMap = new HashMap<>();

        columns.forEach(column ->
                generatorMap.put(column, ValueGenerators.createValueGenerator(column, dataSource, registry)));

        return generatorMap;
    }
}
