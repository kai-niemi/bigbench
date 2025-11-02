package io.cockroachdb.bigbench.jdbc;

import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.cockroachdb.bigbench.model.Column;
import io.cockroachdb.bigbench.model.Identity;
import io.cockroachdb.bigbench.model.Table;
import io.cockroachdb.bigbench.util.graph.DirectedAcyclicGraph;
import static io.cockroachdb.bigbench.model.IdentityType.sequence;
import static io.cockroachdb.bigbench.model.IdentityType.unordered;
import static io.cockroachdb.bigbench.model.IdentityType.uuid;

public abstract class SchemaExporter {
    private static final Logger logger = LoggerFactory.getLogger(SchemaExporter.class);

    private SchemaExporter() {
    }

    public static Optional<Table> exportTable(DataSource dataSource, String schema,  Predicate<TableModel> predicate) {
        List<TableModel> tables = MetaDataUtils.findTables(dataSource, schema, predicate);
        return toTables(exportSchema(dataSource, tables).nodes()).stream().findAny();
    }

    public static List<Table> exportTablesInTopologicalOrder(DataSource dataSource, String schema, Predicate<TableModel> predicate) {
        List<TableModel> tables = MetaDataUtils.findTables(dataSource, schema, predicate);
        return toTables(exportSchema(dataSource, tables).nodes());
    }

    private static List<Table> toTables(Collection<TableModel> tableModels) {
        List<Table> tables = new ArrayList<>();
        tableModels.forEach(model -> {
            Table table = new Table();
            table.setSchema(model.getSchema());
            table.setName(model.getName());
            table.setColumns(toColumns(model.getColumns()));
            tables.add(table);
        });
        return tables;
    }

    private static List<Column> toColumns(Collection<ColumnModel> columnModels) {
        List<Column> columns = new ArrayList<>();
        columnModels.forEach(columnModel -> {
            Column column = new Column();
            column.setName(columnModel.getName());
            column.setTypeName(columnModel.getJdbcType().getName());
            column.setIdentity(columnModel.getIdentity());
            column.setExpression(columnModel.getExpression());
            columns.add(column);
        });
        return columns;
    }

    private static DirectedAcyclicGraph<TableModel, ForeignKeyModel> exportSchema(
            DataSource dataSource, List<TableModel> tableModels) {
        // First pass, resolve table columns
        tableModels.forEach(tableModel -> {
            logger.info("Introspect: %s.%s".formatted(tableModel.getSchema(), tableModel.getName()));

            MetaDataUtils.listColumns(dataSource, tableModel).forEach(columnModel -> {
                configureColumn(dataSource, columnModel);
                tableModel.addColumn(columnModel);
            });
        });

        // Second pass, resolve foreign keys
        final DirectedAcyclicGraph<TableModel, ForeignKeyModel> graph = new DirectedAcyclicGraph<>();

        // Build DAG from fk relations
        tableModels.forEach(tableModel -> {
            logger.info("Resolving: %s.%s".formatted(tableModel.getSchema(), tableModel.getName()));

            MetaDataUtils.listForeignKeys(dataSource, tableModel.getSchema(), tableModel.getName(),
                    rs -> {
                        graph.addNode(tableModel);

                        while (rs.next()) {
                            ForeignKeyModel fk = new ForeignKeyModel(rs);

                            tableModel.getColumns()
                                    .stream()
                                    .filter(columnModel -> columnModel.getName().equals(fk.getFkColumnName()))
                                    .forEach(columnModel -> {
                                        columnModel.setExpression(null);
                                        columnModel.setComment("foreign key: " + fk.getFkColumnName());
                                    });
                            
                            tableModels.stream()
                                    .filter(x -> x.getName().equalsIgnoreCase(fk.getPkTableName()))
                                    .findFirst().ifPresent(target -> graph.addEdge(tableModel, target, fk));
                        }
                    });

            MetaDataUtils.listPrimaryKeys(dataSource, tableModel.getSchema(), tableModel.getName(),
                    rs -> {
                        while (rs.next()) {
                            PrimaryKeyModel pk = new PrimaryKeyModel(rs);

                            tableModel.getColumns()
                                    .stream()
                                    .filter(column -> column.getName().equalsIgnoreCase(pk.getColumnName()))
                                    .findFirst().ifPresent(SchemaExporter::configureColumnIdentity);
                        }
                    });
        });

        return graph;
    }

    private static void configureColumn(DataSource dataSource, ColumnModel model) {
        int dataType = model.getAttribute("DATA_TYPE", Integer.class);
        String typeName = model.getAttribute("TYPE_NAME", String.class);
        int columnSize = model.getAttribute("COLUMN_SIZE", Integer.class);
        String columnName = model.getAttribute("COLUMN_NAME", String.class);

        Set<String> enumValues = MetaDataUtils.selectEnumValues(dataSource, typeName);
        if (!enumValues.isEmpty()) {
            model.setExpression("selectRandom(" + String.join(",", enumValues) + ")");
        } else {
            JDBCType jdbcType = JDBCType.valueOf(dataType);
            switch (jdbcType) {
                case BIT, BOOLEAN -> model.setExpression("randomBoolean()");
                case TINYINT, SMALLINT, INTEGER -> model.setExpression("randomInt()");
                case BIGINT -> model.setExpression("randomLong()");
                case FLOAT, REAL, DOUBLE -> model.setExpression("randomDouble()");
                case NUMERIC, DECIMAL -> {
                    int digits = model.getAttribute("DECIMAL_DIGITS", Integer.class, 0);
                    double bound = Math.pow(10, columnSize - digits);
                    model.setExpression("randomBigDecimal(0,%d,%d)".formatted((long) bound, digits));
                }
                case CHAR, VARCHAR, LONGVARCHAR -> {
                    if (columnName.matches("email")) {
                        model.setExpression("randomEmail()");
                    } else if (columnName.matches("city")) {
                        model.setExpression("randomCity()");
                    } else if (columnName.matches("name")) {
                        model.setExpression("randomFullName()");
                    } else if (columnName.matches("country")) {
                        model.setExpression("randomCountry()");
                    } else if (columnName.matches("phone")) {
                        model.setExpression("randomPhoneNumber()");
                    } else if (columnName.matches("state")) {
                        model.setExpression("randomState()");
                    } else if (columnName.matches("zip")) {
                        model.setExpression("randomZipCode()");
                    } else if (columnName.matches("currency")) {
                        model.setExpression("randomCurrency()");
                    } else {
                        model.setExpression("randomString(%d)".formatted(Math.min(512, columnSize)));
                    }
                }
                case DATE -> model.setExpression("randomDate()");
                case TIME -> model.setExpression("randomTime()");
                case TIMESTAMP -> model.setExpression("randomDateTime()");
                case BINARY, VARBINARY, LONGVARBINARY -> {
                    columnSize = Math.min(512, columnSize);
                    model.setExpression("base64(randomBytes(%d))".formatted(columnSize));
                }
                case OTHER -> model.setExpression("randomJson(1,1)");
            }
        }
    }

    private static void configureColumnIdentity(ColumnModel column) {
        int dataType = column.getAttribute("DATA_TYPE", Integer.class);

        switch (JDBCType.valueOf(dataType)) {
            case TINYINT, SMALLINT, INTEGER, BIGINT -> {
//                String autoincrement = column.getAttribute("IS_AUTOINCREMENT", String.class);
                String generatedColumn = column.getAttribute("IS_GENERATEDCOLUMN", String.class);

                // https://www.cockroachlabs.com/docs/v23.2/functions-and-operators#id-generation-functions
                String columnDef = column.getAttribute("COLUMN_DEF", String.class);

                Identity identity = new Identity();

                if ("unique_rowid()".equalsIgnoreCase(columnDef)
                    || "unordered_unique_rowid()".equalsIgnoreCase(columnDef)) {
                    identity.setType(unordered);
                    identity.setBatchSize(512);
                } else if ("gen_random_uuid()".equalsIgnoreCase(columnDef)
                           || "uuid_generate_v4()".equalsIgnoreCase(columnDef)) {
                    identity.setType(uuid);
                } else {
                    if ("YES".equalsIgnoreCase(generatedColumn)) {
                        identity.setType(unordered);
                        identity.setBatchSize(512);
                    } else {
                        identity.setType(sequence);
                        identity.setFrom(1);
                        identity.setStep(1);
                    }
                }

                column.setIdentity(identity);
                column.setExpression(null);
            }
            case OTHER -> {
                Identity id = new Identity();
                id.setType(uuid);

                column.setIdentity(id);
                column.setExpression(null);
            }
        }
    }
}
