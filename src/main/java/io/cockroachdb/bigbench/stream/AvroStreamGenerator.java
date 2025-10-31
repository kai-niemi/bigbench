package io.cockroachdb.bigbench.stream;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

import javax.sql.DataSource;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;

import io.cockroachdb.bigbench.generator.ValueGenerator;
import io.cockroachdb.bigbench.model.Column;
import io.cockroachdb.bigbench.model.Table;

public class AvroStreamGenerator extends AbstractStreamGeneratorSupport {
    private final DataSource dataSource;

    private final Table table;

    private final boolean gzip;

    public AvroStreamGenerator(DataSource dataSource, Table table, boolean gzip) {
        this.dataSource = dataSource;
        this.table = table;
        this.gzip =gzip;
    }

    @Override
    public void streamTo(OutputStream outputStream) {
        final AtomicInteger currentRow = new AtomicInteger();
        final List<Column> columns = table.filterColumns(VISIBLE_COLUMN_PREDICATE);
        final Map<Column, ValueGenerator<?>> columnGenerators = columnGenerators(dataSource, columns, currentRow);

        final Schema schema;
        {
            SchemaBuilder.RecordBuilder<Schema> recordBuilder = SchemaBuilder.record(
                    GenericRecord.class.getSimpleName());
            SchemaBuilder.FieldAssembler<Schema> fieldAssembler = recordBuilder
                    .namespace("io.cockroachdb.bigbench").fields();
            columns.forEach(column -> {
                fieldAssembler.name(column.getName()).type().stringType().noDefault();
            });
            schema = fieldAssembler.endRecord();
        }

        try (DataFileWriter<GenericRecord> dataFileWriter
                     = new DataFileWriter<>(datumWriterForClass(GenericRecord.class))) {
            dataFileWriter.create(schema, gzip
                    ? new GZIPOutputStream(outputStream, true) : outputStream);

            for (int i = 0; i < table.parseRowCount(); i++) {
                GenericRecord genericRecord = new GenericData.Record(schema);
                for (Column col : columns) {
                    genericRecord.put(col.getName(), "" + columnGenerators.get(col).nextValue());
                }
                dataFileWriter.append(genericRecord);
                currentRow.incrementAndGet();
            }

            dataFileWriter.flush();
        } catch (IOException e) {
            throw new UncheckedIOException("I/O exception at row " + currentRow.get(), e);
        } catch (Exception e) {
            throw new UndeclaredThrowableException(e, "Exception at row " + currentRow.get());
        }
    }

    private static <T> DatumWriter<T> datumWriterForClass(Class<T> clazz) {
        if (SpecificRecordBase.class.isAssignableFrom(clazz)) {
            return new SpecificDatumWriter<>(clazz);
        }
        if (GenericRecord.class.isAssignableFrom(clazz)) {
            return new GenericDatumWriter<>();
        }
        return new ReflectDatumWriter<>(clazz);
    }
}
