package io.cockroachdb.bigbench.expression;

import java.util.Optional;

/**
 * An interface used by {@link ExpressionParseTreeListener} for resolving identifiers
 * in expressions to object references or callback functions. Object references can primitives of type
 * Number, String, Boolean, LocalDate, LocalTime, LocalDateTime or a collection of these.
 *
 * @author Kai Niemi
 * @see Function
 * @see ExpressionParseTreeListener
 */
public interface ExpressionRegistry {
    /**
     * Resolves a given variable name.
     *
     * @param id the variable identifier
     * @return value resolved, can be null
     */
    Optional<Object> findVariable(String id);

    /**
     * Resolves a function by given identifier.
     *
     * @param id the function identifier
     * @return function resolved
     */
    Optional<FunctionDef> findFunction(String id);

    ExpressionRegistry addVariable(String id, Object value);

    ExpressionRegistry addFunction(String id, Function function);

    ExpressionRegistry addFunction(FunctionDef functionDef);

    Iterable<String> variableNames();

    Iterable<String> functionCategories();

    Iterable<FunctionDef> functionDefinitions(String category);

    Iterable<FunctionDef> functionDefinitions();
}
