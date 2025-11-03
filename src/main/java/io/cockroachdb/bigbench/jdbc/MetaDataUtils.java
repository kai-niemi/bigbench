package io.cockroachdb.bigbench.jdbc;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import javax.sql.DataSource;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.util.StringUtils;

@SuppressWarnings("SqlSourceToSinkFlow")
public abstract class MetaDataUtils {
    private MetaDataUtils() {
    }

    public static List<TableModel> findTables(DataSource dataSource, String schema, Predicate<TableModel> predicate) {
        List<TableModel> tables = new ArrayList<>();

        listTables(dataSource, schema, rs -> {
            while (rs.next()) {
                TableModel tableModel = new TableModel(rs);
                if (predicate.test(tableModel)) {
                    tables.add(tableModel);
                }
            }
        });

        return tables;
    }

    public static void listSchemas(DataSource dataSource, ResultSetHandler handler) {
        new JdbcTemplate(dataSource).execute((ConnectionCallback<Object>) connection -> {
            ResultSet columns = connection.getMetaData().getSchemas();
            handler.process(columns);
            return null;
        });
    }

    public static void listTables(DataSource dataSource, String schema, ResultSetHandler handler) {
        new JdbcTemplate(dataSource).execute((ConnectionCallback<Object>) connection -> {
            ResultSet columns = connection.getMetaData()
                    .getTables(null, "*".equals(schema) ? null : schema, null, new String[] {"TABLE"});
            handler.process(columns);
            return null;
        });
    }

    public static void listPrimaryKeys(DataSource dataSource, String schema, String tableName,
                                       ResultSetHandler handler) {
        new JdbcTemplate(dataSource).execute((ConnectionCallback<Object>) connection -> {
            ResultSet resultSet = connection.getMetaData().getPrimaryKeys(null, schema, stripQuotes(tableName));
            handler.process(resultSet);
            return null;
        });
    }

    public static void listForeignKeys(DataSource dataSource, String schema, String tableName,
                                       ResultSetHandler handler) {
        new JdbcTemplate(dataSource).execute((ConnectionCallback<Object>) connection -> {
            ResultSet resultSet = connection.getMetaData().getImportedKeys(null, schema, stripQuotes(tableName));
            handler.process(resultSet);
            return null;
        });
    }

    public static void listColumns(DataSource dataSource, String schema, String tableName, ResultSetHandler handler) {
        new JdbcTemplate(dataSource).execute((ConnectionCallback<Object>) connection -> {
            ResultSet resultSet = connection.getMetaData().getColumns(null, schema, stripQuotes(tableName), null);
            handler.process(resultSet);
            return null;
        });
    }

    public static List<ColumnModel> listColumns(DataSource dataSource, TableModel table) {
        String tableName = stripQuotes(table.getName());

        List<ColumnModel> columns = new LinkedList<>();
        listColumns(dataSource, table.getSchema(), tableName, resultSet -> {
            while (resultSet.next()) {
                columns.add(new ColumnModel(resultSet));
            }
        });

//        loadComments(dataSource, tableName, rs -> {
//            String columnName = rs.getString("column_name");
//            String comment = rs.getString("comment");
//
//            columns.forEach(column -> {
//                if (column.getName().equals(columnName)) {
//                    column.setComment(comment);
//                }
//            });
//        });

        return columns;
    }

    public static String showCreateTable(DataSource dataSource, String table) {
        JdbcTemplate template = new JdbcTemplate(dataSource);

        String createTable;
        try {
            createTable = template.queryForObject("SELECT create_statement FROM [SHOW CREATE TABLE " + table + "]",
                    String.class).replaceAll("[\r\n\t]+", "");
        } catch (DataAccessException e) {
            // Assuming it's related to escapes since JDBC metadata API removes surrounding quotes
            createTable = template.queryForObject("SELECT create_statement FROM [SHOW CREATE TABLE \"" + table + "\"]",
                    String.class).replaceAll("[\r\n\t]+", "");
        }

        return createTable.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS") + ";";
    }

    public static boolean isCockroachDB(DataSource dataSource) {
        return databaseVersion(dataSource).contains("CockroachDB");
    }

    public static String databaseVersion(DataSource dataSource) {
        try {
            return new JdbcTemplate(dataSource).queryForObject("select version()", String.class);
        } catch (DataAccessException e) {
            return "unknown";
        }
    }

    public static void loadComments(DataSource dataSource, String tableName, RowCallbackHandler handler) {
        JdbcTemplate template = new JdbcTemplate(dataSource);
        try {
            template.query("select column_name,comment from [SHOW COLUMNS FROM "
                           + tableName + " WITH COMMENT] where comment is not null", handler);
        } catch (BadSqlGrammarException e) {
            template.query("select column_name,comment from [SHOW COLUMNS FROM \""
                           + tableName + "\" WITH COMMENT] where comment is not null", handler);
        }
    }

    public static Set<String> selectEnumValues(DataSource dataSource, String tableName) {
        JdbcTemplate template = new JdbcTemplate(dataSource);
        try {
            String values = template.queryForObject(
                    "select array_to_string(values,',') from [SHOW ENUMS] where name = '"
                    + tableName + "'", String.class);
            Set<String> quotedValues = new HashSet<>();
            StringUtils.commaDelimitedListToSet(values).forEach(s -> quotedValues.add("'" + s + "'"));
            return quotedValues;
        } catch (DataAccessException e) {
            return Set.of();
        }
    }

    private static String stripQuotes(String tableName) {
        return tableName.replaceAll("\"", "");
    }
}
