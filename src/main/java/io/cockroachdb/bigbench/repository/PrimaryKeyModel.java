package io.cockroachdb.bigbench.repository;

import java.sql.ResultSet;
import java.sql.SQLException;

public class PrimaryKeyModel extends MetaDataModel {
    public PrimaryKeyModel(ResultSet resultSet) throws SQLException {
        super(resultSet);
    }

    @Override
    public String getName() {
        return getPkName();
    }

    public String getTableName() {
        return getAttribute("TABLE_NAME", String.class);
    }

    public String getColumnName() {
        return getAttribute("COLUMN_NAME", String.class);
    }

    public String getKeySeq() {
        return getAttribute("KEY_SEQ", String.class);
    }

    public String getPkName() {
        return getAttribute("PK_NAME", String.class);
    }
}
