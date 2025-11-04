package io.cockroachdb.bigbench.shell;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ansi.AnsiColor;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.NonTransientDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.data.util.Pair;
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
import io.cockroachdb.bigbench.shell.csv.InstrumentedChunkProcessor;
import io.cockroachdb.bigbench.shell.csv.ProcessorException;
import io.cockroachdb.bigbench.shell.support.AnsiConsole;
import io.cockroachdb.bigbench.shell.support.HypermediaClient;
import io.cockroachdb.bigbench.shell.support.ListTableModel;
import io.cockroachdb.bigbench.shell.support.SchemaNameProvider;
import io.cockroachdb.bigbench.shell.support.TableNameProvider;
import io.cockroachdb.bigbench.shell.support.TableRenderer;
import io.cockroachdb.bigbench.util.AsciiArt;
import io.cockroachdb.bigbench.util.Multiplier;
import io.cockroachdb.bigbench.web.LinkRelations;
import io.cockroachdb.bigbench.web.NotFoundException;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
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

    @Autowired
    private AnsiConsole ansiConsole;

    @Autowired
    private TableRenderer tableRenderer;

    @Autowired
    private MeterRegistry meterRegistry;

    private void go(Link streamingLink, ChunkedCsvStreamReader reader, ChunkProcessor<List<String>> processor) {
        InstrumentedChunkProcessor<List<String>> instrumentedProcessor
                = new InstrumentedChunkProcessor<>(processor, meterRegistry);

        final Instant start = Instant.now();

        Pair<Integer, Integer> result = hypermediaClient.get(streamingLink, httpResponse ->
                reader.readInputStream(httpResponse.getBody(), instrumentedProcessor));

        int rowsProduced = result.getFirst();
        int rowsConsumed = result.getSecond();
        final Duration duration = Duration.between(start, Instant.now());
        final HistogramSnapshot snapshot = instrumentedProcessor.getProcessingTime().takeSnapshot();

        final String percentiles = Arrays.stream(snapshot.percentileValues())
                .map(valueAtPercentile -> String.format("P%s (%.1f ms) ",
                        valueAtPercentile.percentile() * 100, valueAtPercentile.value(TimeUnit.MILLISECONDS)
                )).collect(Collectors.joining());

        logger.info("""
                ** Finished processing input stream **
                  Produced rows: %,d
                  Consumed rows: %,d
                       Duration: %s
                     Throughput: %.1f rows/s
                         Chunks: %,d
                     Total time: %.1f s
                      Mean time: %.1f ms
                       Max time: %.1f ms
                    Percentiles: %s
                    %s""".formatted(
                rowsProduced,
                rowsConsumed,
                duration,
                rowsConsumed / Math.max(1, (duration.toMillis() / 1000.0) % 60),
                snapshot.count(),
                snapshot.total(TimeUnit.SECONDS),
                snapshot.mean(TimeUnit.MILLISECONDS),
                snapshot.max(TimeUnit.MILLISECONDS),
                percentiles,
                rowsProduced != (rowsConsumed + 1) ? AsciiArt.flipTableRoughly() : AsciiArt.shrug())
        );
    }

    @ShellMethod(value = "Batch INSERT from CSV stream", key = {"batch-insert", "bi"})
    public void batchInsert(@ShellOption(help = "table schema", defaultValue = "public",
                                    valueProvider = SchemaNameProvider.class) String schema,
                            @ShellOption(help = "table name(s)",
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
        Link streamingLink = hypermediaClient.from(Link.of(endpoint))
                .follow(rel(curied(CURIE_NAMESPACE, LinkRelations.TABLE_REL).value())
                        .withParameter("schema", schema)
                        .withParameter("table", table))
                .follow(rel(curied(CURIE_NAMESPACE, LinkRelations.CSV_STREAM_REL).value())
                        .withParameter("rows", rows)
                        .withParameter("delimiter", delimiter)
                )
                .asTemplatedLink();

        go(streamingLink, new ChunkedCsvStreamReader(batchSize, delimiter),
                new BatchInsertProcessor(schema, table, onConflictDoNothing, upsert, dataSource));
    }

    @ShellMethod(value = "Array INSERT from CSV stream", key = {"array-insert", "ai"})
    public void arrayInsert(@ShellOption(help = "table schema", defaultValue = "public",
                                    valueProvider = SchemaNameProvider.class) String schema,
                            @ShellOption(help = "table name(s)",
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
        Link streamingLink = hypermediaClient.from(Link.of(endpoint))
                .follow(rel(curied(CURIE_NAMESPACE, LinkRelations.TABLE_REL).value())
                        .withParameter("schema", schema)
                        .withParameter("table", table))
                .follow(rel(curied(CURIE_NAMESPACE, LinkRelations.CSV_STREAM_REL).value())
                        .withParameter("rows", rows))
                .asTemplatedLink();

        go(streamingLink, new ChunkedCsvStreamReader(batchSize, delimiter),
                new ArrayInsertProcessor(schema, table, onConflictDoNothing, upsert, dataSource));
    }

    @ShellMethod(value = "Singleton INSERT from CSV stream", key = {"singleton-insert", "si"})
    public void singletonInsert(@ShellOption(help = "table schema", defaultValue = "public",
                                        valueProvider = SchemaNameProvider.class) String schema,
                                @ShellOption(help = "table name(s)",
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
        Link streamingLink = hypermediaClient.from(Link.of(endpoint))
                .follow(rel(curied(CURIE_NAMESPACE, LinkRelations.TABLE_REL).value())
                        .withParameter("schema", schema)
                        .withParameter("table", table))
                .follow(rel(curied(CURIE_NAMESPACE, LinkRelations.CSV_STREAM_REL).value())
                        .withParameter("rows", rows))
                .asTemplatedLink();

        go(streamingLink, new ChunkedCsvStreamReader(batchSize, delimiter),
                new SingletonInsertProcessor(schema, table, onConflictDoNothing, upsert, dataSource));
    }

    @ShellMethod(value = "Download and print CSV stream", key = {"download-csv", "dc"})
    public void downloadCSV(@ShellOption(help = "table schema", defaultValue = "public",
                                    valueProvider = SchemaNameProvider.class) String schema,
                            @ShellOption(help = "table name(s)",
                                    valueProvider = TableNameProvider.class) String table,
                            @ShellOption(help = "number of rows to retrieve", defaultValue = "100") String rows,
                            @ShellOption(help = "batch/chunk size", defaultValue = "16") int batchSize,
                            @ShellOption(help = "CSV column delimiter", defaultValue = ",") String delimiter,
                            @ShellOption(help = "API root endpoint", defaultValue = "http://localhost:9090/")
                            String endpoint
    ) {
        int rowNum = Multiplier.parseInt(rows);

        Link streamingLink = hypermediaClient.from(Link.of(endpoint))
                .follow(rel(curied(CURIE_NAMESPACE, LinkRelations.TABLE_REL).value())
                        .withParameter("schema", schema)
                        .withParameter("table", table))
                .follow(rel(curied(CURIE_NAMESPACE, LinkRelations.CSV_STREAM_REL).value())
                        .withParameter("rows", rowNum)
                        .withParameter("delimiter", delimiter)
                )
                .asTemplatedLink();

        hypermediaClient.get(streamingLink, httpResponse -> {
            List<List<?>> tuples = new ArrayList<>();

            new ChunkedCsvStreamReader(batchSize, delimiter)
                    .readInputStream(httpResponse.getBody(), new ChunkProcessor<>() {
                        @Override
                        public void processHeader(List<String> columns) throws ProcessorException {
                            tuples.add(columns);
                        }

                        @Override
                        public int processChunk(Chunk<? extends List<String>> chunk) throws ProcessorException {
                            chunk.forEach(tuples::add);
                            return chunk.size();
                        }
                    });

            ansiConsole.print(AnsiColor.CYAN, tableRenderer.renderTable(new ListTableModel(tuples))).nl();

            return null;
        });
    }

    @ShellMethod(value = "Set transient SQL error handling strategy", key = {"handle-transient-errors", "te"})
    public void handleTransientErrors(@ShellOption(help = "transient error strategy",
            valueProvider = EnumValueProvider.class) ErrorStrategy strategy) {
        Dml.transientErrorStrategy = strategy;
        logger.info("Using transient strategy: " + strategy);
    }

    @ShellMethod(value = "Set non-transient SQL error handling strategy", key = {"handle-nontransient-errors", "nte"})
    public void handleNonTransientErrors(
            @ShellOption(help = "non-transient error strategy",
                    valueProvider = EnumValueProvider.class) ErrorStrategy strategy) {
        Dml.nonTransientErrorStrategy = strategy;
        logger.info("Using non-transient strategy: " + strategy);
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

            // reWriteBatchedInserts=true will hose rows affected
            long success = Arrays.stream(rv)
                    .filter(value -> value != Statement.EXECUTE_FAILED && value != Statement.SUCCESS_NO_INFO)
                    .sum();

            rows.addAndGet(success > 0 ? (int) success : chunk.size());
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
