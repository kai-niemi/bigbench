package io.cockroachdb.bigbench.generator;

import io.cockroachdb.bigbench.model.Identity;
import io.cockroachdb.bigbench.model.IdentityType;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Deque;
import java.util.LinkedList;

public class RowIdGenerator implements ValueGenerator<Long> {
    private final DataSource dataSource;

    private final String function;

    private final int batchSize;

    private final Deque<Long> uniqueRowIdBuffer = new LinkedList<>();

    public RowIdGenerator(DataSource dataSource, Identity gen) {
        this.dataSource = dataSource;
        this.function = gen.getType().equals(IdentityType.ordered)
                ? "unique_rowid()" : "unordered_unique_rowid()";
        this.batchSize = gen.getBatchSize();
    }

    @Override
    public Long nextValue() {
        if (uniqueRowIdBuffer.isEmpty()) {
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement ps = conn.prepareStatement(
                         "select " + function + " from generate_series(1, " + batchSize + ")")) {
                ps.setFetchSize(batchSize);
                try (ResultSet res = ps.executeQuery()) {
                    while (res.next()) {
                        uniqueRowIdBuffer.add(res.getLong(1));
                    }
                }
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        }
        return uniqueRowIdBuffer.pop();
    }
}
