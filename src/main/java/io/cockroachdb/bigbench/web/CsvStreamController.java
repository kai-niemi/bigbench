package io.cockroachdb.bigbench.web;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
import org.springframework.util.MultiValueMap;
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
import io.cockroachdb.bigbench.generator.ImportInto;
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
            @RequestParam(required = false) MultiValueMap<String, String> valueMap,
            @RequestHeader(value = HttpHeaders.ACCEPT_ENCODING, required = false, defaultValue = "")
            String acceptEncoding) {
        logger.debug(">> streamTableInCSVFormat: schema=%s, name=%s, params=%s, acceptEncoding=%s"
                .formatted(schema, name, valueMap, acceptEncoding));

        final Map<String, String> allParams = Objects.requireNonNull(valueMap, "params required").toSingleValueMap();

        Table table = lookupTable(QualifiedName.of(schema, name));
        table.setRows(allParams.getOrDefault("rows", "10"));

        boolean gzip = acceptEncoding.contains("gzip");

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(TEXT_CSV);
        headers.setContentDisposition(ContentDisposition.inline().build());
        headers.setCacheControl(CacheControl.noCache().noTransform().mustRevalidate());

        if (gzip) {
            headers.set(HttpHeaders.CONTENT_ENCODING, "gzip");
        }

        logger.debug("<< streamTableInCSVFormat: schema=%s, name=%s, params=%s, acceptEncoding=%s"
                .formatted(schema, name, valueMap, acceptEncoding));

        return ResponseEntity.ok()
                .headers(headers)
                .body(outputStream -> new CsvStreamGenerator(dataSource, table, gzip)
                        .setDelimiter(allParams.getOrDefault("delimiter", ","))
                        .setQuoteCharacter(allParams.getOrDefault("quoteCharacter", ""))
                        .setIncludeHeader(Boolean.parseBoolean(allParams.getOrDefault("header", "true")))
                        .streamTo(outputStream));
    }

    @GetMapping(value = "/{name}.csv/form")
    public ResponseEntity<EntityModel<Table>> getTableForm(
            @PathVariable(name = "schema") String schema,
            @PathVariable(name = "name") String name,
            @RequestParam(required = false) MultiValueMap<String, String> valueMap) {
        final Map<String, String> allParams = Objects.requireNonNull(valueMap, "params required").toSingleValueMap();

        Table table = lookupTable(QualifiedName.of(schema, name));
        table.setRows(allParams.getOrDefault("rows", "10"));

        return ResponseEntity.ok(EntityModel.of(table)
                .add(linkTo(methodOn(getClass())
                        .getTableForm(schema, name, valueMap))
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
            @RequestParam(required = false) MultiValueMap<String, String> valueMap) {
        final Map<String, String> allParams = Objects.nonNull(valueMap) ? valueMap.toSingleValueMap() : Map.of();

        Table table = lookupTable(QualifiedName.of(schema, name));

        final String uri = ServletUriComponentsBuilder.fromCurrentContextPath()
                .pathSegment(schema, name + ".csv")
                .queryParams(valueMap)
                .buildAndExpand()
                .toUriString();

        int nodes = Integer.parseInt(allParams.getOrDefault("nodes", "6"));

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
