package io.cockroachdb.bigbench.shell;

public abstract class CommandGroups {
    public static final String ADMIN_COMMANDS = "(01) Admin Commands";

    public static final String LOGGING_COMMANDS = "(02) Logging Commands";

    public static final String EXPRESSION_COMMANDS = "(03) Expression Commands";

    public static final String DATABASE_SCHEMA_COMMANDS = "(04) Database Schema Commands";

    private CommandGroups() {
    }
}
