package io.cockroachdb.bigbench.web;

import org.springframework.hateoas.RepresentationModel;
import org.springframework.hateoas.server.core.Relation;

import com.fasterxml.jackson.annotation.JsonInclude;

import jakarta.validation.constraints.NotNull;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Relation(collectionRelation = "tables")
public class TableForm extends RepresentationModel<TableForm> {
    private String schema;

    private String name;

    public TableForm(String schema, String name) {
        this.schema = schema;
        this.name = name;
    }

    public String getSchema() {
        return schema;
    }

    public String getName() {
        return name;
    }
}
