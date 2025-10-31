package io.cockroachdb.bigbench.shell.support;

import io.cockroachdb.bigbench.repository.MetaDataUtils;
import io.cockroachdb.bigbench.repository.TableModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.CompletionContext;
import org.springframework.shell.CompletionProposal;
import org.springframework.shell.standard.ValueProvider;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;

public class TableNameProvider implements ValueProvider {
    @Autowired
    private DataSource dataSource;

    @Override
    public List<CompletionProposal> complete(CompletionContext completionContext) {
        List<CompletionProposal> result = new ArrayList<>();

        MetaDataUtils.listTables(dataSource, "*", resultSet -> {
            while (resultSet.next()) {
                TableModel table = new TableModel(resultSet);

                String prefix = completionContext.currentWordUpToCursor();
                if (prefix == null) {
                    prefix = "";
                }
                if (table.getName().startsWith(prefix)) {
                    result.add(new CompletionProposal(table.getName()));
                }
            }
        });

        return result;
    }
}
