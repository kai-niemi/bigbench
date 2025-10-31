package io.cockroachdb.bigbench.expression;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.springframework.util.StringUtils;

/**
 * Parse and evaluate logical expressions.
 * <p/>
 * See the ANTLR4 grammar for specifics.
 *
 * @author Kai Niemi
 */
public class Expression {
    public static final ExpressionRegistry EMPTY_REGISTRY = new DefaultExpressionRegistry();

    /**
     * Parse an expression for syntax validation.
     *
     * @param expression the expression
     * @return true if the expression is valid
     */
    public static boolean isValid(String expression) {
        if (!StringUtils.hasLength(expression)) {
            return false;
        }
        try {
            evaluate(expression, Object.class, EMPTY_REGISTRY);
            return true;
        } catch (ExpressionException ex) {
            return false;
        }
    }

    /**
     * Parse and evaluate an expression.
     *
     * @param expression the expression
     * @return the binary outcome
     * @throws ExpressionException if the expression break grammar rules
     */
    public static Object evaluate(String expression) {
        return evaluate(expression, Object.class);
    }

    public static Object evaluate(String expression, ExpressionRegistry registry) {
        return evaluate(expression, Object.class, registry);
    }

    /**
     * Parse and evaluate an expression.
     *
     * @param expression the expression
     * @param type       the type that the result object is expected to match
     * @return the result object of the expression
     * @throws ExpressionException if the expression break grammar rules
     */
    public static <T> T evaluate(String expression, Class<T> type) {
        return evaluate(expression, type, EMPTY_REGISTRY);
    }

    /**
     * Parse and evaluate an expression.
     *
     * @param expression the expression
     * @param type       the type that the result object is expected to match
     * @param registry   callback for resolving expression variables and functions
     * @return the result object of the expression
     * @throws ExpressionException if the expression break grammar rules
     */
    public static <T> T evaluate(String expression, Class<T> type, ExpressionRegistry registry) {
        ExpressionParser parser = createParser(expression);

        ExpressionParseTreeListener listener = new ExpressionParseTreeListener(parser, registry);
        parser.addParseListener(listener);
        parser.root();

        return type.cast(listener.popFinal());
    }

    private static ExpressionParser createParser(String expression) {
        final FailFastErrorStrategy errorStrategy = new FailFastErrorStrategy();

        ExpressionLexer lexer
                = new ExpressionLexer(CharStreams.fromString(expression));
        lexer.removeErrorListeners();
        lexer.addErrorListener(errorStrategy);

        ExpressionParser parser
                = new ExpressionParser(new CommonTokenStream(lexer));
        parser.setErrorHandler(errorStrategy);
        parser.addErrorListener(errorStrategy);
        // Grammar is simple enough with low ambiguity level
        parser.getInterpreter().setPredictionMode(PredictionMode.SLL);

        return parser;
    }
}