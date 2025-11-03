package io.cockroachdb.bigbench.shell;

import java.lang.reflect.InvocationTargetException;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.DatabasePopulatorUtils;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.shell.standard.ShellCommandGroup;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;
import org.springframework.util.ReflectionUtils;
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
import io.cockroachdb.bigbench.shell.support.TableRenderer;
import io.cockroachdb.bigbench.util.graph.DirectedAcyclicGraph;

@ShellComponent
@ShellCommandGroup(CommandGroups.DATABASE_SCHEMA_COMMANDS)
public class Database {
    @Autowired
    private DataSource dataSource;

    @Autowired
    private AnsiConsole ansiConsole;

    @Autowired
    private TableRenderer tableRenderer;

    @ShellMethod(value = "Print database metadata", key = {"metadata", "m"})
    public void metadata(
            @ShellOption(help = "Include all no-arg database metadata methods", defaultValue = "false") boolean all) {
        JdbcTemplate template = new JdbcTemplate(dataSource);
        template.execute((ConnectionCallback<Object>) connection -> {
            DatabaseMetaData metaData = connection.getMetaData();

            Map<String, Object> properties = new TreeMap<>();
            if (all) {
                ReflectionUtils.doWithMethods(java.sql.DatabaseMetaData.class, method -> {
                    if (method.getParameterCount() == 0) {
                        try {
                            Object rv = method.invoke(metaData);
                            properties.put(method.getName(), rv);
                        } catch (InvocationTargetException e) {
                            properties.put(method.getName(), e.getTargetException().getMessage());
                        }
                    }
                });
            } else {
                properties.put("databaseProductName", metaData.getDatabaseProductName());
                properties.put("databaseMajorVersion", metaData.getDatabaseMajorVersion());
                properties.put("databaseMinorVersion", metaData.getDatabaseMinorVersion());
                properties.put("databaseProductVersion", metaData.getDatabaseProductVersion());
                properties.put("driverMajorVersion", metaData.getDriverMajorVersion());
                properties.put("driverMinorVersion", metaData.getDriverMinorVersion());
                properties.put("driverName", metaData.getDriverName());
                properties.put("driverVersion", metaData.getDriverVersion());
                properties.put("maxConnections", metaData.getMaxConnections());
                properties.put("defaultTransactionIsolation", metaData.getDefaultTransactionIsolation());
                properties.put("transactionIsolation", connection.getTransactionIsolation());
            }

            properties.forEach((k, v) -> ansiConsole.yellow("%s = ", k).cyan("%s", v).nl());

            return null;
        });
    }

    @ShellMethod(value = "Execute SQL script", key = {"exec-file", "ef"})
    public void executeScript(@ShellOption(help = "path to DDL/DML file",
            valueProvider = AnotherFileValueProvider.class) String path) {

        ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
        populator.addScript(new FileSystemResource(path));
        populator.setCommentPrefixes("--", "#");
        populator.setIgnoreFailedDrops(false);
        populator.setContinueOnError(false);

        DatabasePopulatorUtils.execute(populator, dataSource);
    }

    @ShellMethod(value = "Print connection pool statistics", key = {"pool-stats", "ps"})
    public void connectionPoolStats() {
        try {
            HikariDataSource hikariDataSource = dataSource.unwrap(HikariDataSource.class);

            List<List<?>> tuples = new ArrayList<>();
            tuples.add(List.of("Key", "Value"));
            tuples.add(List.of("name", hikariDataSource.getPoolName()));
            tuples.add(List.of("maximumPoolSize", hikariDataSource.getMaximumPoolSize()));
            tuples.add(List.of("minimumIdle", hikariDataSource.getMinimumIdle()));
            tuples.add(List.of("idleTimeout", hikariDataSource.getIdleTimeout()));
            tuples.add(List.of("loginTimeout", hikariDataSource.getLoginTimeout()));
            tuples.add(List.of("maxLifetime", hikariDataSource.getMaxLifetime()));
            tuples.add(List.of("validationTimeout", hikariDataSource.getValidationTimeout()));
            tuples.add(List.of("lLeakDetectionThreshold", hikariDataSource.getLeakDetectionThreshold()));
            tuples.add(List.of("autoCommit", hikariDataSource.isAutoCommit()));
            tuples.add(List.of("allowPoolSuspension", hikariDataSource.isAllowPoolSuspension()));
            tuples.add(List.of("readOnly", hikariDataSource.isReadOnly()));
            tuples.add(List.of("running", hikariDataSource.isRunning()));
            tuples.add(List.of("properties", hikariDataSource.getDataSourceProperties()));

            ansiConsole.yellow("Configuration:").nl();
            ansiConsole.cyan(tableRenderer.renderTable(new ListTableModel(tuples)
            )).nl();

            HikariPoolMXBean hikariPool = hikariDataSource.getHikariPoolMXBean();

            tuples.clear();
            tuples.add(List.of("Key", "Value"));
            tuples.add(List.of("activeConnections", hikariPool.getActiveConnections()));
            tuples.add(List.of("idleConnections", hikariPool.getIdleConnections()));
            tuples.add(List.of("threadsAwaitingConnection", hikariPool.getThreadsAwaitingConnection()));
            tuples.add(List.of("totalConnections", hikariPool.getTotalConnections()));

            ansiConsole.yellow("Metrics:").nl();
            ansiConsole.cyan(tableRenderer.renderTable(new ListTableModel(tuples)
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
            @ShellOption(help = "table schema", defaultValue = "public") String schema) {
        MetaDataUtils.listTables(dataSource, schema, resultSet -> {
            try {
                ansiConsole.cyan(tableRenderer.renderTable(resultSet, List.of()));
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

        MetaDataUtils.findTables(dataSource, schema,
                        model -> "*".equals(tableName) || model.getName().equalsIgnoreCase(tableName))
                .forEach(tableModel -> {
                    ansiConsole.yellow("Table '%s':", tableModel.getName()).nl();

                    MetaDataUtils.listColumns(dataSource, tableModel.getSchema(), tableModel.getName(),
                            resultSet -> {
                                try {
                                    ansiConsole.cyan(tableRenderer.renderTable(resultSet,
                                            List.of("COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE",
                                                    "DECIMAL_DIGITS", "NULLABLE", "REMARKS", "COLUMN_DEF",
                                                    "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN"))).nl();
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
        List<List<?>> tuples = new ArrayList<>();
        tuples.add(List.of("Key", "Value"));

        MetaDataUtils.findTables(dataSource, schema,
                        model -> "*".equals(tableName) || model.getName().equalsIgnoreCase(tableName))
                .forEach(tableModel -> {
                    tuples.add(List.of(tableModel.getName(),
                            MetaDataUtils.showCreateTable(dataSource, tableModel.getName())));

                });

        ansiConsole.yellow("Tables:").nl();
        ansiConsole.cyan(tableRenderer.renderTable(new ListTableModel(tuples)
        )).nl();
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
                    ansiConsole.yellow("Table '%s':", tableModel.getName()).nl();

                    MetaDataUtils.listForeignKeys(dataSource, tableModel.getSchema(), tableModel.getName(),
                            resultSet -> {
                                try {
                                    ansiConsole.cyan(tableRenderer.renderTable(resultSet, List.of())).nl();
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
                    ansiConsole.yellow("Table '%s':", tableModel.getName()).nl();

                    MetaDataUtils.listPrimaryKeys(dataSource, tableModel.getSchema(), tableModel.getName(),
                            resultSet -> {
                                try {
                                    ansiConsole.cyan(tableRenderer.renderTable(resultSet, List.of()));
                                } catch (SQLException e) {
                                    throw new CommandException(e);
                                }
                            });
                });
    }

    @ShellMethod(value = "Print table topology", key = {"topology", "top"})
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
