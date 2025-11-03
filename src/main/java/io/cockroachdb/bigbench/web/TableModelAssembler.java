package io.cockroachdb.bigbench.web;

import org.springframework.hateoas.MediaTypes;
import org.springframework.hateoas.server.RepresentationModelAssembler;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;

import io.cockroachdb.bigbench.model.Table;
import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.linkTo;
import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.methodOn;

@Component
public class TableModelAssembler implements RepresentationModelAssembler<Table, TableForm> {
    @Override
    public TableForm toModel(Table entity) {
        TableForm resource = new TableForm(entity.getSchema(), entity.getName());

        resource.add(linkTo(methodOn(TableController.class)
                .getTable(entity.getSchema(), entity.getName()))
                .withSelfRel());

        {
            resource.add(linkTo(methodOn(CsvStreamController.class)
                    .streamTableInCSVFormat(entity.getSchema(), entity.getName(),
                            null, null, null, null, null))
                    .withRel(LinkRelations.CSV_STREAM_REL)
                    .withType(CsvStreamController.TEXT_CSV_VALUE)
                    .withTitle("Export table stream in CSV text format"));
            resource.add(linkTo(methodOn(CsvStreamController.class)
                    .getImportInto(entity.getSchema(), entity.getName(),
                            null, null, null, null, null))
                    .withRel(LinkRelations.CSV_IMPORT_REL)
                    .withType(MediaType.TEXT_PLAIN_VALUE)
                    .withTitle("Generate IMPORT INTO statement"));
            resource.add(linkTo(methodOn(CsvStreamController.class)
                    .getTableForm(entity.getSchema(), entity.getName(), null))
                    .withRel(LinkRelations.CSV_FORM_REL)
                    .withType(MediaTypes.HAL_FORMS_JSON_VALUE)
                    .withTitle("Table form")
            );
        }

        {
            resource.add(linkTo(methodOn(AvroStreamController.class)
                    .streamTableInAvroFormat(entity.getSchema(), entity.getName(), null, null))
                    .withRel(LinkRelations.AVRO_STREAM_REL)
                    .withType(AvroStreamController.APPLICATION_AVRO_SCHEMA_VALUE)
                    .withTitle("Export table stream in Avro OCF format"));
            resource.add(linkTo(methodOn(AvroStreamController.class)
                    .getImportInto(entity.getSchema(), entity.getName(), null, null))
                    .withRel(LinkRelations.AVRO_IMPORT_REL)
                    .withType(MediaType.TEXT_PLAIN_VALUE)
                    .withTitle("Generate IMPORT INTO statement"));
            resource.add(linkTo(methodOn(AvroStreamController.class)
                    .getTableForm(entity.getSchema(), entity.getName(), null))
                    .withRel(LinkRelations.AVRO_FORM_REL)
                    .withType(MediaTypes.HAL_FORMS_JSON_VALUE)
                    .withTitle("Table form")
            );
        }

        return resource;
    }
}
