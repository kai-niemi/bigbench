package io.cockroachdb.bigbench.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonInclude;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

import io.cockroachdb.bigbench.util.Multiplier;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Table {
    @NotNull
    private String schema;

    @NotNull
    private String name;

    @Pattern(regexp = "^[+-]?([0-9]+\\.?[0-9]*|\\.[0-9]+)\\s?([kKmMgG]+)?")
    private String rows;

    @NotEmpty
    private List<@Valid Column> columns = new ArrayList<>();

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getRows() {
        return rows;
    }

    public void setRows(String rows) {
        this.rows = rows;
    }

    public List<Column> getColumns() {
        return Collections.unmodifiableList(columns);
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }

    public int parseRowCount() {
        return rows != null ? Multiplier.parseInt(rows) : 0;
    }

    public List<Column> filterColumns(Predicate<Column> filter) {
        return columns.stream().filter(filter).toList();
    }

    public List<String> columnNames() {
        return columns.stream()
                .map(Column::getName)
                .collect(Collectors.toList());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Table table = (Table) o;
        return Objects.equals(name, table.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
