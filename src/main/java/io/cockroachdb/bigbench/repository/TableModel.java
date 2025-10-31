package io.cockroachdb.bigbench.repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class TableModel extends MetaDataModel {
    private final String name;

    private final String schema;

    private  List<ColumnModel> columns = new ArrayList<>();

    public TableModel(ResultSet resultSet) throws SQLException {
        super(resultSet);
        this.name = resultSet.getString("TABLE_NAME").toLowerCase();
        this.schema = resultSet.getString("TABLE_SCHEM").toLowerCase();
    }

    public void addColumn(ColumnModel columnModel) {
        this.columns.add(columnModel);
    }

    public List<ColumnModel> getColumns() {
        return columns;
    }

    public void setColumns(List<ColumnModel> columns) {
        this.columns = columns;
    }

    @Override
    public String getName() {
        return name;
    }

    public String getSchema() {
        return schema;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;

        TableModel that = (TableModel) o;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }
}
