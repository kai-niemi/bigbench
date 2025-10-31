package io.cockroachdb.bigbench.shell.support;

import org.springframework.shell.table.BorderStyle;
import org.springframework.shell.table.TableBuilder;
import org.springframework.shell.table.TableModel;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public abstract class TableUtils {
    private TableUtils() {
    }

    public static String prettyPrint(ResultSet resultSet)
            throws SQLException {
        return prettyPrint(resultSet, rs -> true);
    }

    public static String prettyPrint(ResultSet resultSet, Predicate<ResultSet> predicate)
            throws SQLException {
        List<Object> headers = new ArrayList<>();
        List<List<Object>> data = new ArrayList<>();

        ResultSetMetaData metaData = resultSet.getMetaData();
        final int columnCount = metaData.getColumnCount();
        for (int col = 1; col <= columnCount; col++) {
            headers.add(metaData.getColumnName(col));
        }

        data.add(headers);

        while (resultSet.next()) {
            if (predicate.test(resultSet)) {
                List<Object> row = new ArrayList<>();
                for (int col = 1; col <= columnCount; col++) {
                    row.add(resultSet.getObject(col));
                }
                data.add(row);
            }
        }

        return prettyPrint(new TableModel() {
            @Override
            public int getRowCount() {
                return data.size();
            }

            @Override
            public int getColumnCount() {
                return columnCount;
            }

            @Override
            public Object getValue(int row, int column) {
                List<Object> rowData = data.get(row);
                return rowData.get(column);
            }
        });
    }

    public static String prettyPrint(TableModel model) {
        TableBuilder tableBuilder = new TableBuilder(model);
        tableBuilder.addInnerBorder(BorderStyle.fancy_light);
        tableBuilder.addHeaderBorder(BorderStyle.fancy_double);
        return tableBuilder.build().render(120);
    }
}
