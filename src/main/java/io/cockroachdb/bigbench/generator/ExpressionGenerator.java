package io.cockroachdb.bigbench.generator;

import io.cockroachdb.bigbench.model.Column;
import io.cockroachdb.bigbench.expression.ExpressionRegistry;
import io.cockroachdb.bigbench.expression.Expression;
import org.springframework.util.StringUtils;

public class ExpressionGenerator implements ValueGenerator<Object> {
    private final Column column;

    private final ExpressionRegistry registry;

    public ExpressionGenerator(Column column, ExpressionRegistry registry) {
        this.column = column;
        this.registry = registry;
    }

    @Override
    public Object nextValue() {
        String expression = column.getExpression();
        if (StringUtils.hasLength(expression)) {
            return Expression.evaluate(expression, Object.class, registry);
        }
        throw new IllegalStateException("Undefined column value generator for: "
                + column.getName());
    }

}
