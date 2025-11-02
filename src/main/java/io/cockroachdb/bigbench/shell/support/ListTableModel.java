package io.cockroachdb.bigbench.shell.support;

import java.util.ArrayList;
import java.util.List;

import org.springframework.shell.table.TableModel;

public class ListTableModel extends TableModel {
    private final List<List<?>> data = new ArrayList<>();

    private final List<Object> headerRow;

    public ListTableModel(List<List<?>> data, List<Object> header) {
        this.data.addAll(data);
        this.headerRow = new ArrayList<>(header);
    }

    @Override
    public int getRowCount() {
        return 1 + data.size();
    }

    @Override
    public int getColumnCount() {
        return headerRow.size();
    }

    @Override
    public Object getValue(int row, int column) {
        if (headerRow != null && row == 0) {
            return headerRow.get(column);
        }
        int rowToUse = headerRow == null ? row : row - 1;
        return data.get(rowToUse).get(column);
    }
}
