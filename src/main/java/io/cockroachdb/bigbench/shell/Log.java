package io.cockroachdb.bigbench.shell;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import io.cockroachdb.bigbench.config.DataSourceConfiguration;
import io.cockroachdb.bigbench.shell.support.AnsiConsole;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.standard.ShellCommandGroup;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;

@ShellComponent
@ShellCommandGroup(CommandGroups.LOGGING_COMMANDS)
public class Log {
    @Autowired
    private AnsiConsole ansiConsole;

    @ShellMethod(value = "Toggle SQL trace logging", key = {"toggle-sql", "ts"})
    public void toggleSQLTraceLogging() {
        setLogLevel(DataSourceConfiguration.SQL_TRACE_LOGGER, Level.DEBUG, Level.TRACE);
    }

    @ShellMethod(value = "Toggle debug logging", key = {"toggle-debug", "td"})
    public void toggleDebugLogging() {
        setLogLevel("io.cockroachdb.bigbench", Level.INFO, Level.DEBUG);
    }

    @ShellMethod(value = "Toggle progress bar", key = {"toggle-progress", "tp"})
    public void toggleProgressBar() {
        boolean onOff = ansiConsole.toggleProgressBar();
        ansiConsole.blue("Progress bar %s", onOff ? "on" : "off").nl();
    }

    private Level setLogLevel(String name, Level precondition, Level newLevel) {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger logger = loggerContext.getLogger(name);
        setLogLevel(logger, logger.getEffectiveLevel().isGreaterOrEqual(precondition) ? newLevel : precondition);
        return logger.getLevel();
    }

    private Level setLogLevel(Logger logger, Level newLevel) {
        logger.setLevel(newLevel);
        ansiConsole.blue("'%s' level set to %s\n", logger.getName(), logger.getLevel());
        return logger.getLevel();
    }
}
