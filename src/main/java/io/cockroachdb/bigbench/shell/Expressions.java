package io.cockroachdb.bigbench.shell;

import java.time.Duration;
import java.time.Instant;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.standard.ShellCommandGroup;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import io.cockroachdb.bigbench.expression.Expression;
import io.cockroachdb.bigbench.expression.ExpressionException;
import io.cockroachdb.bigbench.expression.ExpressionRegistry;
import io.cockroachdb.bigbench.expression.ExpressionRegistryBuilder;
import io.cockroachdb.bigbench.shell.support.AnsiConsole;
import io.cockroachdb.bigbench.shell.support.FunctionValueProvider;

@ShellComponent
@ShellCommandGroup(CommandGroups.EXPRESSION_COMMANDS)
public class Expressions {
    @Autowired
    private DataSource dataSource;

    @Autowired
    private AnsiConsole ansiConsole;

    @ShellMethod(value = "Evaluate expression", key = {"eval", "e"})
    public void evaluateFunction(@ShellOption(
            value = {"expression"},
            valueProvider = FunctionValueProvider.class) String expression) {

        final ExpressionRegistry registry = ExpressionRegistryBuilder.build(dataSource);

        ansiConsole.magenta("Expression: ").green("%s", expression).nl();

        try {
            Instant now = Instant.now();
            Object result = Expression.evaluate(expression, registry);
            Duration duration = Duration.between(now, Instant.now());

            ansiConsole.magenta("Result: ")
                    .green("%s", result)
                    .yellow(" (%s)", result.getClass().getName())
                    .nl()
                    .magenta("Time: ")
                    .green(" %s", duration.toString())
                    .nl();
        } catch (ExpressionException e) {
            ansiConsole.red("ERROR: %s", e.getMessage()).nl();
        }
    }

    @ShellMethod(value = "List expression functions and variables", key = {"functions", "f"})
    public void listFunctions() {
        final ExpressionRegistry registry = ExpressionRegistryBuilder.build(dataSource);

        ansiConsole.cyan("-- Functions --").nl();
        registry.functionCategories().forEach(category -> {
            ansiConsole.green("--- Category '%s' ---", category).nl();
            registry.functionDefinitions(category).forEach(fn -> {
                ansiConsole.magenta("%s", fn.toString()).nl();
            });
        });

        ansiConsole.cyan("-- Variables --").nl();
        registry.variableNames().forEach(fn -> {
            ansiConsole.magenta("%s", fn).nl();
        });
    }
}
