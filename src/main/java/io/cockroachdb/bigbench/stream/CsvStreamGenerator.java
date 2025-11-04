package io.cockroachdb.bigbench.stream;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

import javax.sql.DataSource;

import io.cockroachdb.bigbench.model.Column;
import io.cockroachdb.bigbench.model.Table;
import io.cockroachdb.bigbench.stream.generator.ValueGenerator;

public class CsvStreamGenerator extends AbstractStreamGeneratorSupport implements StreamGenerator {
    private String delimiter = ",";

    private String quoteCharacter = "";

    private boolean includeHeader;

    private final DataSource dataSource;

    private final Table table;

    private final boolean gzip;

    public CsvStreamGenerator(DataSource dataSource, Table table, boolean gzip) {
        this.dataSource = dataSource;
        this.table = table;
        this.gzip = gzip;
    }

    public CsvStreamGenerator setDelimiter(String delimiter) {
        this.delimiter = delimiter;
        return this;
    }

    public CsvStreamGenerator setIncludeHeader(boolean includeHeader) {
        this.includeHeader = includeHeader;
        return this;
    }

    public CsvStreamGenerator setQuoteCharacter(String quoteCharacter) {
        this.quoteCharacter = quoteCharacter;
        return this;
    }

    @Override
    public void streamTo(OutputStream outputStream) {
        final AtomicInteger currentRow = new AtomicInteger();
        final List<Column> columns = table.filterColumns(VISIBLE_COLUMN_PREDICATE);
        final Map<Column, ValueGenerator<?>> columnGenerators = columnGenerators(dataSource, columns, currentRow);

        try (PrintWriter writer = new PrintWriter(gzip
                ? new GZIPOutputStream(outputStream, true) : outputStream)) {
            if (includeHeader) {
                String header = columns.stream()
                        .map(Column::getName)
                        .collect(Collectors.joining(this.delimiter));
                writer.println(header);
            }

            for (int i = 0; i < table.parseRowCount(); i++) {
                Object[] fields = new Object[columns.size()];

                int j = 0;
                for (Column col : columns) {
                    Object field = columnGenerators.get(col).nextValue();
                    fields[j++] = field;
                }

                String line = Arrays.stream(fields)
                        .map(field -> this.quoteCharacter + field + this.quoteCharacter)
                        .collect(Collectors.joining(this.delimiter));
                writer.println(line);

                currentRow.incrementAndGet();
            }
            writer.flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (Exception e) {
            throw new UndeclaredThrowableException(e);
        }
    }
}
