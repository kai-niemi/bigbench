package io.cockroachdb.bigbench.shell.support;

import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.CompletionContext;
import org.springframework.shell.CompletionProposal;
import org.springframework.shell.standard.ValueProvider;

import io.cockroachdb.bigbench.jdbc.MetaDataUtils;

public class SchemaNameProvider implements ValueProvider {
    @Autowired
    private DataSource dataSource;

    @Override
    public List<CompletionProposal> complete(CompletionContext completionContext) {
        List<CompletionProposal> result = new ArrayList<>();

        MetaDataUtils.listSchemas(dataSource, resultSet -> {
            while (resultSet.next()) {
                String schema = resultSet.getString("TABLE_SCHEM");

                String prefix = completionContext.currentWordUpToCursor();
                if (prefix == null) {
                    prefix = "";
                }
                if (schema.startsWith(prefix)) {
                    result.add(new CompletionProposal(schema));
                }
            }
        });

        return result;
    }
}
