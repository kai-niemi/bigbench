package io.cockroachdb.bigbench.expression;

/**
 * An interface used by {@link ExpressionParseTreeListener} for invoking
 * callback functions.
 *
 * @author Kai Niemi
 */
@FunctionalInterface
public interface Function {
    /**
     * Invoke the function.
     *
     * @param args arguments passed in expression
     * @return the resulting object
     * @throws Exception on any exception error
     */
    Object call(Object... args) throws Exception;
}
