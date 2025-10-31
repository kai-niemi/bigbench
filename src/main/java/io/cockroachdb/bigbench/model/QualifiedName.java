package io.cockroachdb.bigbench.model;

import java.util.Objects;

import jakarta.validation.constraints.NotNull;

public class QualifiedName implements Comparable<QualifiedName> {
    public static QualifiedName of(String schema, String table) {
        return new QualifiedName(schema, table);
    }

    @NotNull
    private final String schema;

    @NotNull
    private final String table;

    public QualifiedName(String schema, String table) {
        Objects.requireNonNull(schema);
        Objects.requireNonNull(table);
        this.schema = schema;
        this.table = table;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    @Override
    public int compareTo(QualifiedName o) {
        return this.toString().compareTo(o.toString());
    }

    @Override
    public String toString() {
        return schema + "." + table;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;

        QualifiedName that = (QualifiedName) o;
        return schema.equals(that.schema) && table.equals(that.table);
    }

    @Override
    public int hashCode() {
        int result = schema.hashCode();
        result = 31 * result + table.hashCode();
        return result;
    }
}
