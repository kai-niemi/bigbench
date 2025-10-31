package io.cockroachdb.bigbench.expression;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

import javax.sql.DataSource;

public abstract class SQLFunctions {
    private SQLFunctions() {
    }

    public static Function selectOne(DataSource dataSource) {
        return args -> {
            LinkedList<Object> argsList = new LinkedList<>(Arrays.asList(args));
            String query = (String) argsList.pop();

            List<Object> result = new ArrayList<>();
            try (Connection conn = dataSource.getConnection()) {
                conn.setReadOnly(true);
                conn.setAutoCommit(true);

                //noinspection SqlSourceToSinkFlow
                try (PreparedStatement ps = conn.prepareStatement(query)) {
                    int col = 1;
                    for (Object a : argsList) {
                        ps.setObject(col++, a);
                    }

                    try (ResultSet res = ps.executeQuery()) {
                        while (res.next()) {
                            result.add(res.getObject(1));
                        }
                    }
                }
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }

            if (result.size() != 1) {
                throw new IllegalStateException("Expected 1 row got " + result.size());
            }

            return result.iterator().next();
        };
    }

    public static Function unorderedUniqueRowId(DataSource dataSource) {
        Deque<Long> uniqueRowIdBuffer = new LinkedList<>();
                      
        return args -> {
            Number batchSize = (Number) args[0];
            if (uniqueRowIdBuffer.isEmpty()) {
                try (Connection conn = dataSource.getConnection();
                     PreparedStatement ps = conn.prepareStatement(
                             "select unordered_unique_rowid() from generate_series(1, "
                                     + batchSize.intValue() + ")")) {
                    ps.setFetchSize(batchSize.intValue());
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
        };
    }

    public static Function uniqueRowId(DataSource dataSource) {
        Deque<Long> uniqueRowIdBuffer = new LinkedList<>();

        return args -> {
            Number batchSize = (Number) args[0];
            if (uniqueRowIdBuffer.isEmpty()) {
                try (Connection conn = dataSource.getConnection();
                     PreparedStatement ps = conn.prepareStatement(
                             "select unique_rowid() from generate_series(1, "
                                     + batchSize.intValue() + ")")) {
                    ps.setFetchSize(batchSize.intValue());
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
        };
    }
}
