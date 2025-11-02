package io.cockroachdb.bigbench.shell;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.datasource.init.DatabasePopulatorUtils;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.shell.standard.ShellCommandGroup;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;
import org.springframework.util.StringUtils;

import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;

import io.cockroachdb.bigbench.jdbc.ForeignKeyModel;
import io.cockroachdb.bigbench.jdbc.MetaDataUtils;
import io.cockroachdb.bigbench.jdbc.TableModel;
import io.cockroachdb.bigbench.shell.support.AnotherFileValueProvider;
import io.cockroachdb.bigbench.shell.support.AnsiConsole;
import io.cockroachdb.bigbench.shell.support.ListTableModel;
import io.cockroachdb.bigbench.shell.support.TableNameProvider;
import io.cockroachdb.bigbench.shell.support.TableUtils;
import io.cockroachdb.bigbench.util.graph.DirectedAcyclicGraph;

@ShellComponent
@ShellCommandGroup(CommandGroups.DATABASE_SCHEMA_COMMANDS)
public class Database {
    @Autowired
    private DataSource dataSource;

    @Autowired
    private AnsiConsole ansiConsole;

    @ShellMethod(value = "Execute SQL file", key = {"exec-file", "ef"})
    public void createSchema(@ShellOption(help = "path to DDL/DML file",
            valueProvider = AnotherFileValueProvider.class) String path) {

        ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
        populator.addScript(new FileSystemResource(path));
        populator.setCommentPrefixes("--", "#");
        populator.setIgnoreFailedDrops(false);
        populator.setContinueOnError(false);

        DatabasePopulatorUtils.execute(populator, dataSource);
    }

    @ShellMethod(value = "Print connection pool stats", key = {"pool-stats", "ps"})
    public void connectionPoolStats() {
        try {
            HikariDataSource hikariDataSource = dataSource.unwrap(HikariDataSource.class);

            List<List<?>> tuples = new ArrayList<>();
            tuples.add(List.of("name", hikariDataSource.getPoolName()));
            tuples.add(List.of("maximumPoolSize", hikariDataSource.getMaximumPoolSize()));
            tuples.add(List.of("minimumIdle", hikariDataSource.getMinimumIdle()));
            tuples.add(List.of("idleTimeout", hikariDataSource.getIdleTimeout()));
            tuples.add(List.of("loginTimeout", hikariDataSource.getLoginTimeout()));
            tuples.add(List.of("maxLifetime", hikariDataSource.getMaxLifetime()));
            tuples.add(List.of("validationTimeout", hikariDataSource.getValidationTimeout()));

            ansiConsole.yellow("Configuration:").nl();
            ansiConsole.cyan(TableUtils.prettyPrint(
                    new ListTableModel(tuples, List.of("Property", "Value"))
            )).nl();

            HikariPoolMXBean hikariPool = hikariDataSource.getHikariPoolMXBean();

            tuples.clear();
            tuples.add(List.of("activeConnections", hikariPool.getActiveConnections()));
            tuples.add(List.of("idleConnections", hikariPool.getIdleConnections()));
            tuples.add(List.of("threadsAwaitingConnection", hikariPool.getThreadsAwaitingConnection()));
            tuples.add(List.of("totalConnections", hikariPool.getTotalConnections()));

            ansiConsole.yellow("Metrics:").nl();
            ansiConsole.cyan(TableUtils.prettyPrint(
                    new ListTableModel(tuples, List.of("Property", "Value"))
            )).nl();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @ShellMethod(value = "Print database version", key = {"db-version", "dbv"})
    public void version() {
        ansiConsole.cyan("%s", MetaDataUtils.databaseVersion(dataSource)).nl();
    }

    @ShellMethod(value = "List tables", key = {"tables", "t"})
    public void listTables(
            @ShellOption(help = "table schema", defaultValue = "public") String schema,
            @ShellOption(help = "table name(s)", defaultValue = "*",
                    valueProvider = TableNameProvider.class) String tableNames) {
        Set<String> names = StringUtils.commaDelimitedListToSet(tableNames.toLowerCase());

        MetaDataUtils.listTables(dataSource, schema, resultSet -> {
            try {
                ansiConsole.cyan(TableUtils.prettyPrint(resultSet, rs -> {
                    try {
                        return names.contains("*")
                               || names.contains(rs.getString("TABLE_NAME").toLowerCase());
                    } catch (SQLException e) {
                        return false;
                    }
                }));
            } catch (SQLException e) {
                throw new CommandException(e);
            }
        });
    }

    @ShellMethod(value = "List columns", key = {"columns", "c"})
    public void listColumns(
            @ShellOption(help = "table schema", defaultValue = "public") String schema,
            @ShellOption(help = "table name", defaultValue = "*", valueProvider = TableNameProvider.class)
            String tableName) {
        MetaDataUtils.findTables(dataSource, schema, model -> model.getName().equalsIgnoreCase(tableName))
                .forEach(tableModel -> {
                    ansiConsole.magenta("%s:", tableModel).nl();

                    MetaDataUtils.listColumns(dataSource, tableModel.getSchema(), tableModel.getName(), resultSet -> {
                        try {
                            ansiConsole.cyan(TableUtils.prettyPrint(resultSet)).nl();
                        } catch (SQLException e) {
                            throw new CommandException(e);
                        }
                    });
                });
    }

    @ShellMethod(value = "Show create table", key = {"show-table", "st"})
    public void showCreateTable(
            @ShellOption(help = "table schema", defaultValue = "public") String schema,
            @ShellOption(help = "table name", defaultValue = "*", valueProvider = TableNameProvider.class)
            String tableName) {
        MetaDataUtils.findTables(dataSource, schema, model -> model.getName().equalsIgnoreCase(tableName))
                .forEach(model -> {
                    ansiConsole.cyan(MetaDataUtils.showCreateTable(dataSource, model.getName())).nl().nl();
                });
    }

    @ShellMethod(value = "List foreign keys", key = {"foreign-keys", "fk"})
    public void listForeignKeys(
            @ShellOption(help = "table schema", defaultValue = "public") String schema,
            @ShellOption(help = "table name", defaultValue = "*", valueProvider = TableNameProvider.class)
            String tableName) {
        MetaDataUtils.findTables(dataSource, schema, model ->
                        tableName.equals("*") ||
                        model.getName().equalsIgnoreCase(tableName))
                .forEach(tableModel -> {
                    MetaDataUtils.listForeignKeys(dataSource, tableModel.getSchema(), tableModel.getName(),
                            resultSet -> {
                                try {
                                    ansiConsole.cyan(TableUtils.prettyPrint(resultSet)).nl();
                                } catch (SQLException e) {
                                    throw new CommandException(e);
                                }
                            });
                });
    }

    @ShellMethod(value = "List primary keys", key = {"primary-keys", "pk"})
    public void listPrimaryKeys(
            @ShellOption(help = "table schema", defaultValue = "public") String schema,
            @ShellOption(help = "table name", defaultValue = "*", valueProvider = TableNameProvider.class)
            String tableName) {
        MetaDataUtils.findTables(dataSource, schema, model ->
                        tableName.equals("*") ||
                        model.getName().equalsIgnoreCase(tableName))
                .forEach(tableModel -> {
                    MetaDataUtils.listPrimaryKeys(dataSource, tableModel.getSchema(), tableModel.getName(),
                            resultSet -> {
                                try {
                                    ansiConsole.cyan(TableUtils.prettyPrint(resultSet));
                                } catch (SQLException e) {
                                    throw new CommandException(e);
                                }
                            });
                });
    }

    @ShellMethod(value = "Print table topology", key = {"topology", "tt"})
    public void listTopology(@ShellOption(help = "table schema", defaultValue = "public") String schema,
                             @ShellOption(help = "table name", defaultValue = "*", valueProvider = TableNameProvider.class)
                             String tableNames) {

        final Set<String> names = StringUtils.commaDelimitedListToSet(tableNames.toLowerCase());

        final DirectedAcyclicGraph<String, ForeignKeyModel> directedAcyclicGraph = new DirectedAcyclicGraph<>();

        MetaDataUtils.listTables(dataSource, schema, resultSet -> {
            while (resultSet.next()) {
                TableModel tableModel = new TableModel(resultSet);

                if (names.contains("*") || names.contains(tableModel.getName())) {
                    directedAcyclicGraph.addNode(tableModel.getName());
                }

                MetaDataUtils.listForeignKeys(dataSource, tableModel.getSchema(), tableModel.getName(), rs -> {
                    while (rs.next()) {
                        ForeignKeyModel fk = new ForeignKeyModel(rs);
                        directedAcyclicGraph.addNode(fk.getPkTableName());
                        directedAcyclicGraph.addEdge(fk.getFkTableName(), fk.getPkTableName(), fk);
                    }
                });
            }
        });

        ansiConsole.green("Topological order (inverse):").nl()
                .cyan(directedAcyclicGraph.topologicalSort(true).toString()).nl();
    }
}
