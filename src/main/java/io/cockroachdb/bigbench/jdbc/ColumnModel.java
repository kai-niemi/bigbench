package io.cockroachdb.bigbench.jdbc;

import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;

import io.cockroachdb.bigbench.model.Identity;

public class ColumnModel extends MetaDataModel {
    private Identity identity;

    private String comment;

    private String expression;

    public ColumnModel(ResultSet resultSet) throws SQLException {
        super(resultSet);
    }

    public JDBCType getJdbcType() {
        int dataType = getAttribute("DATA_TYPE", Integer.class);
        return JDBCType.valueOf(dataType);
    }

    public boolean isGenerated() {
        return getAttribute("is_generatedcolumn", String.class, "NO")
                .equalsIgnoreCase("YES");
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    @Override
    public String getName() {
        return getAttribute("COLUMN_NAME", String.class);
    }

    public Identity getIdentity() {
        return identity;
    }

    public void setIdentity(Identity identity) {
        this.identity = identity;
    }
}
