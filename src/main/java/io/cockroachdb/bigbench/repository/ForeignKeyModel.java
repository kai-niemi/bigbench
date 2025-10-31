package io.cockroachdb.bigbench.repository;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ForeignKeyModel extends MetaDataModel {
    public ForeignKeyModel(ResultSet resultSet) throws SQLException {
        super(resultSet);
    }

    @Override
    public String getName() {
        return getFkName();
    }

    public String getRemarks() {
        return getAttribute("REMARKS", String.class);
    }

    public String getFkName() {
        return getAttribute("FK_NAME", String.class);
    }

    public String getPkTableName() {
        return getAttribute("PKTABLE_NAME", String.class);
    }

    public String getFkTableName() {
        return getAttribute("FKTABLE_NAME", String.class);
    }

    public String getPkColumnName() {
        return getAttribute("PKCOLUMN_NAME", String.class);
    }

    public String getFkColumnName() {
        return getAttribute("FKCOLUMN_NAME", String.class);
    }
}
