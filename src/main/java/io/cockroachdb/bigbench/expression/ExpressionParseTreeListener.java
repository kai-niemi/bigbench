package io.cockroachdb.bigbench.expression;

import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.ParserRuleContext;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Objects;

/**
 * ANTLR4 parse tree listener that evaluate logical and binary
 * expressions accordingly to grammar rules.
 *
 * @author Kai Niemi
 */
public class ExpressionParseTreeListener extends ExpressionParserBaseListener {
    private static String stripQuotes(String s) {
        return s.replaceAll("(^')|('$)", "");
    }

    private final Parser parser;

    private final ExpressionRegistry registry;

    private final Deque<Object> stack = new ArrayDeque<>();

    private final Deque<Object> binaryOutcome = new ArrayDeque<>();

    public ExpressionParseTreeListener(Parser parser, ExpressionRegistry registry) {
        this.parser = parser;
        this.registry = registry;
    }

    public Object popFinal() {
        if (!binaryOutcome.isEmpty()) {
            Boolean top = pop(Boolean.class);
            return top ? binaryOutcome.getLast() : binaryOutcome.getFirst();
        }
        return pop(Object.class);
    }

    private void push(Object o) {
        if (o instanceof Number && !(o instanceof BigDecimal)) {
            this.stack.push(new BigDecimal(Objects.toString(o)));
        } else {
            this.stack.push(o);
        }
    }

    private <T> T pop(Class<T> type) {
        Object top = this.stack.pop();
        try {
            return type.cast(top);
        } catch (ClassCastException e) {
            throw ExpressionException.from(parser, "Cannot cast '" + top + "' of type "
                    + top.getClass().getSimpleName() + " into " + type.getSimpleName());
        }
    }

    private <T> T pop(Class<T> type, ParserRuleContext ctx) {
        Object top = this.stack.pop();
        try {
            return type.cast(top);
        } catch (ClassCastException e) {
            throw ExpressionException.from(parser, "Cannot cast '" + top + "' of type "
                    + top.getClass().getSimpleName() + " into " + type.getSimpleName(), ctx.getStop());
        }
    }

    private <T> void compareAndPush(ExpressionParser.Comp_operatorContext op,
                                    Comparable<T> left, T right) {
        if (Objects.nonNull(op.GT())) {
            push(left.compareTo(right) > 0);
        } else if (Objects.nonNull(op.GE())) {
            push(left.compareTo(right) >= 0);
        } else if (Objects.nonNull(op.LT())) {
            push(left.compareTo(right) < 0);
        } else if (Objects.nonNull(op.LE())) {
            push(left.compareTo(right) <= 0);
        } else if (Objects.nonNull(op.EQ())) {
            push(left.compareTo(right) == 0);
        } else if (Objects.nonNull(op.NE())) {
            push(left.compareTo(right) != 0);
        } else {
            throw ExpressionException.from(parser,
                    "Unknown comparison operator: " + op.getText(), op.getStop());
        }
    }

    @Override
    public void exitComparisonExpressionStringList(ExpressionParser.ComparisonExpressionStringListContext ctx) {
        boolean match = false;
        String key = (String) stack.pollLast();

        while (!stack.isEmpty()) {
            String v = pop(String.class, ctx);
            if (Objects.requireNonNull(key).equals(v)) {
                match = true;
            }
        }

        push(match);
    }

    @Override
    public void exitArithmeticMultiplicationOrDivision(
            ExpressionParser.ArithmeticMultiplicationOrDivisionContext ctx) {
        BigDecimal right = pop(BigDecimal.class, ctx);
        BigDecimal left = pop(BigDecimal.class, ctx);

        if (Objects.nonNull(ctx.MULT())) {
            push(left.multiply(right));
        } else {
            push(left.divide(right, RoundingMode.HALF_EVEN));
        }
    }

    @Override
    public void exitStringPlus(ExpressionParser.StringPlusContext ctx) {
        String right = pop(String.class, ctx);
        String left = pop(String.class, ctx);

        push(left + right);
    }

    @Override
    public void exitArithmeticPlusOrMinus(ExpressionParser.ArithmeticPlusOrMinusContext ctx) {
        BigDecimal right = pop(BigDecimal.class, ctx);
        BigDecimal left = pop(BigDecimal.class, ctx);

        if (Objects.nonNull(ctx.PLUS())) {
            push(left.add(right));
        } else {
            push(left.subtract(right));
        }
    }

    @Override
    public void exitArithmeticUnaryMinusOrPlus(ExpressionParser.ArithmeticUnaryMinusOrPlusContext ctx) {
        BigDecimal right = pop(BigDecimal.class, ctx);

        if (Objects.nonNull(ctx.MINUS())) {
            push(right.negate());
        } else {
            push(right);
        }
    }

    @Override
    public void exitArithmeticPower(ExpressionParser.ArithmeticPowerContext ctx) {
        BigDecimal right = pop(BigDecimal.class, ctx);
        BigDecimal left = pop(BigDecimal.class, ctx);

        if (right.stripTrailingZeros().scale() > 0) {
            throw ExpressionException.from(parser,
                    "Floating-point power exponents are not supported: " + right, ctx.getStop());
        }

        push(left.pow(right.intValue()));
    }

    @Override
    public void exitArithmeticMinOrMax(ExpressionParser.ArithmeticMinOrMaxContext ctx) {
        BigDecimal right = pop(BigDecimal.class, ctx);
        BigDecimal left = pop(BigDecimal.class, ctx);

        if (Objects.nonNull(ctx.MIN())) {
            push(left.min(right));
        } else {
            push(left.max(right));
        }
    }

    @Override
    public void exitArithmeticModulus(ExpressionParser.ArithmeticModulusContext ctx) {
        BigDecimal right = pop(BigDecimal.class, ctx);
        BigDecimal left = pop(BigDecimal.class, ctx);

        push(left.remainder(right));
    }

    @Override
    public void exitStringLiteral(ExpressionParser.StringLiteralContext ctx) {
        String text = ctx.getText();

        if (!text.isEmpty()) {
            push(text.substring(1, text.length() - 1));
        } else {
            push("");
        }
    }

    @Override
    public void exitDecimalLiteral(ExpressionParser.DecimalLiteralContext ctx) {
        try {
            push(new BigDecimal(ctx.getText()));
        } catch (NumberFormatException e) {
            throw ExpressionException.from(parser, e, ctx.getStop());
        }
    }

    @Override
    public void exitDateTimeLiteral(ExpressionParser.DateTimeLiteralContext ctx) {
        try {
            String dt = stripQuotes(ctx.DateTimeLiteral().getText());
            push(LocalDateTime.parse(dt.replace(" ", "T")));
        } catch (DateTimeParseException e) {
            throw ExpressionException.from(parser, e, ctx.getStop());
        }
    }

    @Override
    public void exitBooleanLiteral(ExpressionParser.BooleanLiteralContext ctx) {
        String b = ctx.BooleanLiteral().getText();
        push(Boolean.parseBoolean(b));
    }

    @Override
    public void exitDateLiteral(ExpressionParser.DateLiteralContext ctx) {
        try {
            String dt = stripQuotes(ctx.DateLiteral().getText());
            push(LocalDate.parse(dt));
        } catch (DateTimeParseException e) {
            throw ExpressionException.from(parser, e, ctx.getStop());
        }
    }

    @Override
    public void exitTimeLiteral(ExpressionParser.TimeLiteralContext ctx) {
        try {
            String dt = stripQuotes(ctx.TimeLiteral().getText());
            push(LocalTime.parse(dt));
        } catch (DateTimeParseException e) {
            throw ExpressionException.from(parser, e, ctx.getStop());
        }
    }

    @Override
    public void exitFunction(ExpressionParser.FunctionContext ctx) {
        List<Object> args = new ArrayList<>();

        try {
            ctx.functionArguments().functionArgument()
                    .forEach(expressionContext -> args.add(pop(Object.class, ctx)));

            Collections.reverse(args);

            String id = ctx.Identifier().getText();

            FunctionDef functionDef = registry.findFunction(id).orElseThrow(() ->
                    ExpressionException.from(parser, "No such function: " + id));

            Object rv = functionDef.getFunction()
                    .call(args.toArray());
            push(rv);
        } catch (Exception e) {
            throw ExpressionException.from(parser, e, ctx.getStop());
        }
    }

    @Override
    public void exitIdentifier(ExpressionParser.IdentifierContext ctx) {
        String id = ctx.Identifier().getText();
        push(registry.findVariable(id)
                .orElseThrow(() -> ExpressionException.from(parser, "No such variable: " + id)));
    }

    @Override
    public void exitOutcome(ExpressionParser.OutcomeContext ctx) {
        binaryOutcome.push(pop(Object.class, ctx));
    }

    @Override
    public void exitLogicalExpressionAnd(ExpressionParser.LogicalExpressionAndContext ctx) {
        Boolean right = pop(Boolean.class, ctx);
        Boolean left = pop(Boolean.class, ctx);

        push(left && right);
    }

    @Override
    public void exitLogicalExpressionNot(ExpressionParser.LogicalExpressionNotContext ctx) {
        Boolean right = pop(Boolean.class, ctx);

        push(!right);
    }

    @Override
    public void exitLogicalExpressionOr(ExpressionParser.LogicalExpressionOrContext ctx) {
        Boolean right = pop(Boolean.class, ctx);
        Boolean left = pop(Boolean.class, ctx);

        push(left || right);
    }

    @Override
    public void exitComparisonExpressionOperand(ExpressionParser.ComparisonExpressionOperandContext ctx) {
        Object right = pop(Object.class, ctx);
        @SuppressWarnings("unchecked")
        Comparable<Object> left = pop(Comparable.class, ctx);

        compareAndPush(ctx.op, left, right);
    }

    @Override
    public void exitComparison_operand(ExpressionParser.Comparison_operandContext ctx) {
        push(pop(Object.class, ctx));
    }

    @Override
    public void exitComparisonExpressionDate(ExpressionParser.ComparisonExpressionDateContext ctx) {
        LocalDate right = pop(LocalDate.class, ctx);
        LocalDate left = pop(LocalDate.class, ctx);

        compareAndPush(ctx.op, left, right);
    }

    @Override
    public void exitComparisonExpressionTime(ExpressionParser.ComparisonExpressionTimeContext ctx) {
        LocalTime right = pop(LocalTime.class, ctx);
        LocalTime left = pop(LocalTime.class, ctx);

        compareAndPush(ctx.op, left, right);
    }

    @Override
    public void exitComparisonExpressionDateTime(ExpressionParser.ComparisonExpressionDateTimeContext ctx) {
        LocalDateTime right = pop(LocalDateTime.class, ctx);
        LocalDateTime left = pop(LocalDateTime.class, ctx);

        compareAndPush(ctx.op, left, right);
    }
}
