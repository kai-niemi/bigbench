package io.cockroachdb.bigbench.shell.support;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.table.BorderStyle;
import org.springframework.shell.table.TableBuilder;
import org.springframework.shell.table.TableModel;
import org.springframework.stereotype.Component;

@Component
public class TableRenderer {
    @Autowired
    private AnsiConsole ansiConsole;

    private boolean transpose;

    private int width = 120;

    public void toggleTranspose() {
        this.transpose = !this.transpose;
    }

    public String renderTable(ResultSet resultSet, List<String> columnNames) throws SQLException {
        final List<String> headers = new ArrayList<>();

        final ResultSetMetaData metaData = resultSet.getMetaData();

        for (int col = 1; col <= metaData.getColumnCount(); col++) {
            String name = metaData.getColumnName(col);
            if (columnNames.isEmpty() || columnNames.contains(name)) {
                headers.add(name);
            }
        }

        final List<List<?>> data = new ArrayList<>();
        data.add(headers);

        while (resultSet.next()) {
            List<Object> row = new ArrayList<>();
            for (String h : headers) {
                row.add(resultSet.getObject(h));
            }
            data.add(row);
        }

        return renderTable(new ListTableModel(data));
    }

    public String renderTable(TableModel model) {
        return transpose
                ? new TableBuilder(model.transpose())
                .addInnerBorder(BorderStyle.fancy_light)
                .build()
                .render(width)
                : new TableBuilder(model)
                .addInnerBorder(BorderStyle.fancy_light)
                .addHeaderBorder(BorderStyle.fancy_double)
                .build()
                .render(width);
    }

    public void printTable(TableModel model) {
        ansiConsole.cyan(renderTable(model)).nl();
    }
}
