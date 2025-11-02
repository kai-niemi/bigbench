package io.cockroachdb.bigbench.web;

import java.util.List;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.hateoas.CollectionModel;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import io.cockroachdb.bigbench.config.ProfileNames;
import io.cockroachdb.bigbench.model.Table;
import io.cockroachdb.bigbench.jdbc.SchemaExporter;
import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.linkTo;
import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.methodOn;

@RestController
@Profile(ProfileNames.HTTP)
public class TableController {
    @Autowired
    private DataSource dataSource;

    @Autowired
    private TableModelAssembler tableModelAssembler;

    @GetMapping(value = "/{schema}")
    public ResponseEntity<CollectionModel<TableForm>> index(
            @PathVariable("schema") String schema) {
        List<Table> tables = SchemaExporter.exportTablesInTopologicalOrder(
                dataSource, schema, tableModel -> true);

        return ResponseEntity.ok(tableModelAssembler.toCollectionModel(tables)
                .add(linkTo(methodOn(TableController.class)
                        .index(schema))
                        .withSelfRel())
        );
    }

    @GetMapping(value = "/{schema}/{table}")
    public ResponseEntity<TableForm> getTable(
            @PathVariable(name = "schema") String schema,
            @PathVariable(name = "table") String name) {
        // First export schema by introspection
        Table table = SchemaExporter.exportTable(
                        dataSource, schema, model -> name.equalsIgnoreCase(model.getName()))
                .orElseThrow(() -> new NotFoundException("No such table: %s.%s".formatted(schema, name)));
        table.setRows("10");
        return ResponseEntity.ok(tableModelAssembler.toModel(table));
    }
}
