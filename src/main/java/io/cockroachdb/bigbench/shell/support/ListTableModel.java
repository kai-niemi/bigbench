package io.cockroachdb.bigbench.shell.support;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.springframework.shell.table.TableModel;

public class ListTableModel extends TableModel {
    private final List<List<?>> rows;

    public ListTableModel(List<List<?>> rows) {
        this.rows = new ArrayList<>(rows);
    }

    @Override
    public int getRowCount() {
        return rows.size();
    }

    @Override
    public int getColumnCount() {
        return !rows.isEmpty() ? rows.getFirst().size() : 0;
    }

    @Override
    public Object getValue(int row, int column) {
        return rows.get(row).get(column);
    }

    @Override
    public TableModel transpose() {
        final List<List<?>> flattened = new ArrayList<>();

        int r = 0;
        for (List<?> row : this.rows.stream().skip(1).toList()) {
            r++;
            flattened.add(List.of("[Record " + r + "]"));

            int h = 0;
            for (Object col : row) {
                flattened.add(List.of(rows.getFirst().get(h++), Objects.isNull(col) ? "(null)" : col));
            }
        }

        return new TableModel() {
            @Override
            public int getRowCount() {
                return flattened.size();
            }

            @Override
            public int getColumnCount() {
                return 2;
            }

            @Override
            public Object getValue(int row, int column) {
                List<?> r = flattened.get(row);
                return r.size() > 1 ? r.get(column) : column == 0 ? r.getFirst() : "";
            }
        };
    }
}
