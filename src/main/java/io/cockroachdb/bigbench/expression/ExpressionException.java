package io.cockroachdb.bigbench.expression;

import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.Token;

/**
 * Exception thrown when an expression violates the grammar or cannot
 * be compiled.
 *
 * @author Kai Niemi
 */
public class ExpressionException extends RuntimeException {
    public static ExpressionException from(Parser parser, Throwable cause, Token token) {
        parser.removeParseListeners();

        String line = token.getInputStream().toString();

        return new ExpressionException(cause.getMessage() +
                ". Near token '" +
                token.getText() +
                "' at position " +
                token.getCharPositionInLine() +
                " in '" + line + "'", cause);
    }

    public static ExpressionException from(Parser parser, String message, Token token) {
        parser.removeParseListeners();

        String line = token.getInputStream().toString();

        return new ExpressionException(message +
                ". Near token '" +
                token.getText() +
                "' at position " +
                token.getCharPositionInLine() +
                " in '" + line + "'");
    }

    public static ExpressionException from(Parser parser, String message) {
        parser.removeParseListeners();

        Token token = parser.getCurrentToken();
        String line = token.getInputStream().toString();

        return new ExpressionException(message +
                ". Near token '" +
                token.getText() +
                "' at position " +
                token.getCharPositionInLine() +
                " in '" + line + "'");
    }

    public ExpressionException(String message) {
        super(message);
    }

    public ExpressionException(String message, Throwable cause) {
        super(message, cause);
    }
}
