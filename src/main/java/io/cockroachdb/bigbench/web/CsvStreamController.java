package io.cockroachdb.bigbench.web;

import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.springframework.context.annotation.Profile;
import org.springframework.hateoas.EntityModel;
import org.springframework.http.CacheControl;
import org.springframework.http.ContentDisposition;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.Assert;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import jakarta.validation.Valid;

import io.cockroachdb.bigbench.config.ProfileNames;
import io.cockroachdb.bigbench.stream.generator.ImportInto;
import io.cockroachdb.bigbench.model.Format;
import io.cockroachdb.bigbench.model.QualifiedName;
import io.cockroachdb.bigbench.model.Table;
import io.cockroachdb.bigbench.stream.CsvStreamGenerator;
import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.afford;
import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.linkTo;
import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.methodOn;

@RestController
@Profile(ProfileNames.HTTP)
@RequestMapping(value = "/{schema}")
public class CsvStreamController extends AbstractStreamController {
    public static final String TEXT_CSV_VALUE = "text/csv";

    public static final MediaType TEXT_CSV = MediaType.parseMediaType(TEXT_CSV_VALUE);

    @GetMapping(value = "/{name}.csv", produces = TEXT_CSV_VALUE)
    public ResponseEntity<StreamingResponseBody> streamTableInCSVFormat(
            @PathVariable(name = "schema") String schema,
            @PathVariable(name = "name") String name,
            @RequestParam(required = false, name = "delimiter", defaultValue = ",") String delimiter,
            @RequestParam(required = false, name = "quoteCharacter", defaultValue = "") String quoteCharacter,
            @RequestParam(required = false, name = "header", defaultValue = "true") String header,
            @RequestParam(required = false, name = "rows", defaultValue = "100") String rows,
            @RequestHeader(value = HttpHeaders.ACCEPT_ENCODING, required = false, defaultValue = "")
            String acceptEncoding) {
        logger.debug("""
                >> streamTableInCSVFormat:
                    schema = %s
                    name = %s
                    delimiter = %s
                    quoteCharacter = %s
                    header = %s
                    rows = %s
                    acceptEncoding = %s"""
                .formatted(schema, name, delimiter, quoteCharacter, header, rows, acceptEncoding));

        Table table = lookupTable(QualifiedName.of(schema, name));
        table.setRows(rows);

        boolean gzip = acceptEncoding.contains("gzip");

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(TEXT_CSV);
        headers.setContentDisposition(ContentDisposition.inline().build());
        headers.setCacheControl(CacheControl.noCache().noTransform().mustRevalidate());

        if (gzip) {
            headers.set(HttpHeaders.CONTENT_ENCODING, "gzip");
        }

        return ResponseEntity.ok()
                .headers(headers)
                .body(outputStream -> new CsvStreamGenerator(dataSource, table, gzip)
                        .setDelimiter(URLDecoder.decode(delimiter, Charset.defaultCharset()))
                        .setQuoteCharacter(URLDecoder.decode(quoteCharacter, Charset.defaultCharset()))
                        .setIncludeHeader(Boolean.parseBoolean(header))
                        .streamTo(outputStream));
    }

    @GetMapping(value = "/{name}.csv/form")
    public ResponseEntity<EntityModel<Table>> getTableForm(
            @PathVariable(name = "schema") String schema,
            @PathVariable(name = "name") String name,
            @RequestParam(required = false, name = "rows", defaultValue = "100") String rows) {
        Table table = lookupTable(QualifiedName.of(schema, name));
        table.setRows(rows);

        return ResponseEntity.ok(EntityModel.of(table)
                .add(linkTo(methodOn(getClass())
                        .getTableForm(schema, name, rows))
                        .withSelfRel()
                        .andAffordance(afford(methodOn(getClass())
                                .submitForm(schema, name, null)))));
    }

    @PostMapping(value = "/{name}.csv/form", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> submitForm(
            @PathVariable(name = "schema") String schema,
            @PathVariable(name = "name") String name,
            @Valid @RequestBody Table table) {
        Assert.state(schema.equalsIgnoreCase(table.getSchema()), "Schema mismatch");
        Assert.state(name.equalsIgnoreCase(table.getName()), "Table name mismatch");

        putTable(QualifiedName.of(schema, name), table);

        return ResponseEntity.ok().body(null);
    }

    @GetMapping(value = "/{name}/csv/import-into.sql", produces = MediaType.TEXT_PLAIN_VALUE)
    public ResponseEntity<String> getImportInto(
            @PathVariable(name = "schema") String schema,
            @PathVariable(name = "name") String name,
            @RequestParam(required = false, name = "delimiter", defaultValue = ",") String delimiter,
            @RequestParam(required = false, name = "quoteCharacter", defaultValue = "") String quoteCharacter,
            @RequestParam(required = false, name = "header", defaultValue = "true") String header,
            @RequestParam(required = false, name = "rows", defaultValue = "100") String rows,
            @RequestParam(required = false, name = "nodes", defaultValue = "6") Integer nodes
    ) {
        Table table = lookupTable(QualifiedName.of(schema, name));

        final String uri = ServletUriComponentsBuilder.fromCurrentContextPath()
                .pathSegment(schema, name + ".csv")
                .queryParam("delimiter", URLDecoder.decode(delimiter, Charset.defaultCharset()))
                .queryParam("quoteCharacter", URLDecoder.decode(quoteCharacter, Charset.defaultCharset()))
                .queryParam("header", header)
                .queryParam("rows", rows)
                .buildAndExpand()
                .toUriString();

        List<String> paths = IntStream.rangeClosed(1, nodes)
                .mapToObj(value -> uri)
                .collect(Collectors.toCollection(LinkedList::new));

        String sql = ImportInto.builder().withFormat(Format.CSV)
                .withTable(table)
                .withPaths(paths)
                .withOptions(applicationModel.getCsvOptions())
                .build()
                .getImportStatement();

        return ResponseEntity.ok(sql);
    }
}
