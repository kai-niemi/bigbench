package io.cockroachdb.bigbench.shell;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.NonTransientDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.hateoas.Link;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.shell.standard.EnumValueProvider;
import org.springframework.shell.standard.ShellCommandGroup;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import io.cockroachdb.bigbench.jdbc.SchemaExporter;
import io.cockroachdb.bigbench.model.Column;
import io.cockroachdb.bigbench.model.QualifiedName;
import io.cockroachdb.bigbench.model.Table;
import io.cockroachdb.bigbench.shell.csv.Chunk;
import io.cockroachdb.bigbench.shell.csv.ChunkProcessor;
import io.cockroachdb.bigbench.shell.csv.ChunkedCsvStreamReader;
import io.cockroachdb.bigbench.shell.csv.ErrorHandler;
import io.cockroachdb.bigbench.shell.csv.ErrorStrategy;
import io.cockroachdb.bigbench.shell.csv.ProcessorException;
import io.cockroachdb.bigbench.shell.support.HypermediaClient;
import io.cockroachdb.bigbench.shell.support.SchemaNameProvider;
import io.cockroachdb.bigbench.shell.support.TableNameProvider;
import io.cockroachdb.bigbench.util.Multiplier;
import io.cockroachdb.bigbench.web.LinkRelations;
import io.cockroachdb.bigbench.web.NotFoundException;
import static io.cockroachdb.bigbench.web.LinkRelations.CURIE_NAMESPACE;
import static org.springframework.hateoas.client.Hop.rel;
import static org.springframework.hateoas.mediatype.hal.HalLinkRelation.curied;

@ShellComponent
@ShellCommandGroup(CommandGroups.DML_COMMANDS)
public class Dml {
    private static final Logger logger = LoggerFactory.getLogger(Dml.class);

    private static ErrorHandler<DataAccessException> transientErrorStrategy = ErrorStrategy.LOG_AND_CONTINUE;

    private static ErrorHandler<DataAccessException> nonTransientErrorStrategy = ErrorStrategy.RETHROW;

    @Autowired
    private HypermediaClient hypermediaClient;

    @Autowired
    private DataSource dataSource;

    @ShellMethod(value = "Set SQL error handling strategy", key = {"error-strategy", "es"})
    public void toggleErrorStrategy(@ShellOption(help = "transient errors",
                                            valueProvider = EnumValueProvider.class) ErrorStrategy transientErrorStrategy,
                                    @ShellOption(help = "non-transient errors",
                                            valueProvider = EnumValueProvider.class) ErrorStrategy nonTransientErrorStrategy) {
        Dml.transientErrorStrategy = transientErrorStrategy;
        Dml.nonTransientErrorStrategy = nonTransientErrorStrategy;
        logger.info("Using transient strategy: " + transientErrorStrategy);
        logger.info("Using non-transient strategy: " + nonTransientErrorStrategy);
    }

    @ShellMethod(value = "Batch INSERT from CSV stream", key = {"batch-insert", "bi"})
    public void batchInsert(@ShellOption(help = "table schema", defaultValue = "public",
                                    valueProvider = SchemaNameProvider.class) String schema,
                            @ShellOption(help = "table name(s)", defaultValue = "customer",
                                    valueProvider = TableNameProvider.class) String table,
                            @ShellOption(help = "number of rows to retrieve", defaultValue = "1k") String rows,
                            @ShellOption(help = "batch/chunk size", defaultValue = "16") int batchSize,
                            @ShellOption(help = "CSV column delimiter", defaultValue = ",") String delimiter,
                            @ShellOption(help = "add 'ON CONFLICT DO NOTHING' clause", defaultValue = "false")
                            boolean onConflictDoNothing,
                            @ShellOption(help = "use UPSERT instead of INSERT", defaultValue = "false") boolean upsert,
                            @ShellOption(help = "API root endpoint", defaultValue = "http://localhost:9090/")
                            String endpoint
    ) {
        int rowNum = Multiplier.parseInt(rows);

        Link streamingLink = hypermediaClient.from(Link.of(endpoint))
                .follow(rel(curied(CURIE_NAMESPACE, LinkRelations.TABLE_REL).value())
                        .withParameter("schema", schema)
                        .withParameter("table", table))
                .follow(rel(curied(CURIE_NAMESPACE, LinkRelations.CSV_STREAM_REL).value())
                        .withParameter("rows", rowNum))
                .asTemplatedLink();

        hypermediaClient.get(streamingLink, httpResponse -> {
            new ChunkedCsvStreamReader(batchSize, delimiter)
                    .readInputStream(httpResponse.getBody(),
                            new BatchInsertProcessor(schema, table, onConflictDoNothing, upsert, dataSource));
            return null;
        });
    }

    @ShellMethod(value = "Array INSERT from CSV stream", key = {"array-insert", "ai"})
    public void arrayInsert(@ShellOption(help = "table schema", defaultValue = "public",
                                    valueProvider = SchemaNameProvider.class) String schema,
                            @ShellOption(help = "table name(s)", defaultValue = "customer",
                                    valueProvider = TableNameProvider.class) String table,
                            @ShellOption(help = "number of rows to retrieve", defaultValue = "1k") String rows,
                            @ShellOption(help = "batch/chunk size", defaultValue = "16") int batchSize,
                            @ShellOption(help = "CSV column delimiter", defaultValue = ",") String delimiter,
                            @ShellOption(help = "add 'ON CONFLICT DO NOTHING' clause", defaultValue = "false")
                            boolean onConflictDoNothing,
                            @ShellOption(help = "use UPSERT instead of INSERT", defaultValue = "false") boolean upsert,
                            @ShellOption(help = "API root endpoint", defaultValue = "http://localhost:9090/")
                            String endpoint
    ) {
        int rowNum = Multiplier.parseInt(rows);

        Link streamingLink = hypermediaClient.from(Link.of(endpoint))
                .follow(rel(curied(CURIE_NAMESPACE, LinkRelations.TABLE_REL).value())
                        .withParameter("schema", schema)
                        .withParameter("table", table))
                .follow(rel(curied(CURIE_NAMESPACE, LinkRelations.CSV_STREAM_REL).value())
                        .withParameter("rows", rowNum))
                .asTemplatedLink();

        hypermediaClient.get(streamingLink, httpResponse -> {
            new ChunkedCsvStreamReader(batchSize, delimiter)
                    .readInputStream(httpResponse.getBody(),
                            new ArrayInsertProcessor(schema, table, onConflictDoNothing, upsert, dataSource));
            return null;
        });
    }

    @ShellMethod(value = "Singleton INSERT from CSV stream", key = {"singleton-insert", "si"})
    public void singletonInsert(@ShellOption(help = "table schema", defaultValue = "public",
                                        valueProvider = SchemaNameProvider.class) String schema,
                                @ShellOption(help = "table name(s)", defaultValue = "customer",
                                        valueProvider = TableNameProvider.class) String table,
                                @ShellOption(help = "number of rows to retrieve", defaultValue = "1k") String rows,
                                @ShellOption(help = "batch/chunk size", defaultValue = "16") int batchSize,
                                @ShellOption(help = "CSV column delimiter", defaultValue = ",") String delimiter,
                                @ShellOption(help = "add 'ON CONFLICT DO NOTHING' clause", defaultValue = "false")
                                boolean onConflictDoNothing,
                                @ShellOption(help = "use UPSERT instead of INSERT", defaultValue = "false")
                                boolean upsert,
                                @ShellOption(help = "API root endpoint", defaultValue = "http://localhost:9090/")
                                String endpoint
    ) {
        int rowNum = Multiplier.parseInt(rows);

        Link streamingLink = hypermediaClient.from(Link.of(endpoint))
                .follow(rel(curied(CURIE_NAMESPACE, LinkRelations.TABLE_REL).value())
                        .withParameter("schema", schema)
                        .withParameter("table", table))
                .follow(rel(curied(CURIE_NAMESPACE, LinkRelations.CSV_STREAM_REL).value())
                        .withParameter("rows", rowNum))
                .asTemplatedLink();

        hypermediaClient.get(streamingLink, httpResponse -> {
            new ChunkedCsvStreamReader(batchSize, delimiter)
                    .readInputStream(httpResponse.getBody(),
                            new SingletonInsertProcessor(schema, table, onConflictDoNothing, upsert, dataSource));
            return null;
        });
    }

    private static abstract class AbstractInsertChunkProcessor implements ChunkProcessor<List<String>> {
        private final boolean onConflictDoNothing;

        private final boolean upsert;

        private final JdbcTemplate jdbcTemplate;

        private final Table table;

        private String sql;

        public AbstractInsertChunkProcessor(String schema, String table,
                                            boolean onConflictDoNothing, boolean upsert, DataSource dataSource) {
            this.onConflictDoNothing = onConflictDoNothing;
            this.upsert = upsert;

            this.jdbcTemplate = new JdbcTemplate(dataSource);

            this.table = SchemaExporter.exportTable(dataSource, schema,
                            model -> table.equalsIgnoreCase(model.getName()))
                    .orElseThrow(() -> new NotFoundException("No such table: %s"
                            .formatted(QualifiedName.of(schema, table))));
        }

        protected JdbcTemplate getJdbcTemplate() {
            return jdbcTemplate;
        }

        @Override
        public final void processHeader(List<String> columns) {
            if (columns.size() != table.getColumns().size()) {
                throw new RuntimeException("Column count mismatch in CSV stream (%d) vs table schema (%d)"
                        .formatted(columns.size(), table.getColumns().size()));
            }
            this.sql = createSQLStatement(table, onConflictDoNothing, upsert);
        }

        protected String createSQLStatement(Table table, boolean onConflictDoNothing, boolean upsert) {
            String placeHolders = table.getColumnNames()
                    .stream().map(s -> "?").collect(Collectors.joining(","));

            if (upsert) {
                return "UPSERT into %s (%s) values (%s)".formatted(
                        table.getName(), table.getColumnNamesAsString(), placeHolders);
            } else if (onConflictDoNothing) {
                return "INSERT into %s (%s) values (%s) ON CONFLICT DO NOTHING".formatted(
                        table.getName(), table.getColumnNamesAsString(), placeHolders);
            } else {
                return "INSERT into %s (%s) values (%s)".formatted(
                        table.getName(), table.getColumnNamesAsString(), placeHolders);
            }
        }

        @Override
        public int processChunk(Chunk<? extends List<String>> chunk) throws ProcessorException {
            AtomicInteger rows = new AtomicInteger();
            try {
                doProcessChunk(table, sql, chunk, rows);
            } catch (NonTransientDataAccessException e) {
                nonTransientErrorStrategy.handle(this, e);
            } catch (TransientDataAccessException e) {
                transientErrorStrategy.handle(this, e);
            }
            return rows.get();
        }

        protected abstract void doProcessChunk(Table table, String sql, Chunk<? extends List<String>> chunk,
                                               AtomicInteger rows);
    }

    private static class BatchInsertProcessor extends AbstractInsertChunkProcessor {
        public BatchInsertProcessor(String schema, String table,
                                    boolean onConflictDoNothing, boolean upsert, DataSource dataSource) {
            super(schema, table, onConflictDoNothing, upsert, dataSource);
        }

        @Override
        protected void doProcessChunk(Table table, String sql, Chunk<? extends List<String>> chunk,
                                      AtomicInteger rows) {
            final int numCols = table.getColumns().size();

            int[] rv = getJdbcTemplate().batchUpdate(sql, new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    List<String> row = chunk.getItems().get(i);
                    for (int col = 1; col <= numCols; col++) {
                        ps.setObject(col, row.get(col - 1));
                    }
                }

                @Override
                public int getBatchSize() {
                    return chunk.size();
                }
            });

            long success = Arrays.stream(rv)
                    .filter(value -> value != Statement.EXECUTE_FAILED)
                    .sum();

            rows.addAndGet((int) success);
        }
    }

    private static class ArrayInsertProcessor extends AbstractInsertChunkProcessor {
        public ArrayInsertProcessor(String schema, String table,
                                    boolean onConflictDoNothing, boolean upsert, DataSource dataSource) {
            super(schema, table, onConflictDoNothing, upsert, dataSource);
        }

        @Override
        protected String createSQLStatement(Table table, boolean onConflictDoNothing, boolean upsert) {
            String placeHolders = table.getColumnNames()
                    .stream().map(s -> "unnest (?) as " + s).collect(Collectors.joining(","));

            if (upsert) {
                return "UPSERT into %s (%s) SELECT %s".formatted(
                        table.getName(), table.getColumnNamesAsString(), placeHolders);
            } else if (onConflictDoNothing) {
                return "INSERT into %s (%s) SELECT %s ON CONFLICT DO NOTHING".formatted(
                        table.getName(), table.getColumnNamesAsString(), placeHolders);
            } else {
                return "INSERT into %s (%s) SELECT %s".formatted(
                        table.getName(), table.getColumnNamesAsString(), placeHolders);
            }
        }

        @Override
        protected void doProcessChunk(Table table, String sql, Chunk<? extends List<String>> chunk,
                                      AtomicInteger rows) {
            final int numCols = table.getColumns().size();

            Long success = getJdbcTemplate().execute(sql, (PreparedStatementCallback<Long>) ps -> {
                List<List<String>> columnData = new ArrayList<>();

                IntStream.range(0, numCols)
                        .forEach(value -> columnData.add(new ArrayList<>()));

                chunk.forEach(entity -> IntStream.range(0, numCols)
                        .forEach(value -> columnData.get(value)
                                .add(entity.get(value))));

                int i = 1;
                for (List<String> data : columnData) {
                    Column column = table.getColumns().get(i - 1);
                    ps.setArray(i++, ps.getConnection().createArrayOf(column.getTypeName(), data.toArray()));
                }

                return ps.executeLargeUpdate();
            });

            rows.addAndGet(success.intValue());
        }
    }

    private static class SingletonInsertProcessor extends AbstractInsertChunkProcessor {
        public SingletonInsertProcessor(String schema, String table,
                                        boolean onConflictDoNothing, boolean upsert, DataSource dataSource) {
            super(schema, table, onConflictDoNothing, upsert, dataSource);
        }

        @Override
        protected void doProcessChunk(Table table, String sql, Chunk<? extends List<String>> chunk,
                                      AtomicInteger rows) {
            final int numCols = table.getColumns().size();

            for (List<String> row : chunk) {
                int success = getJdbcTemplate().update(sql, ps -> {
                    for (int col = 1; col <= numCols; col++) {
                        ps.setObject(col, row.get(col - 1));
                    }
                });
                rows.addAndGet(success);
            }
        }
    }
}
