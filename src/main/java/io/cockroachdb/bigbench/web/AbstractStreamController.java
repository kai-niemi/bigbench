package io.cockroachdb.bigbench.web;

import java.util.LinkedHashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.RestController;

import io.cockroachdb.bigbench.ApplicationModel;
import io.cockroachdb.bigbench.config.ProfileNames;
import io.cockroachdb.bigbench.model.QualifiedName;
import io.cockroachdb.bigbench.model.Table;
import io.cockroachdb.bigbench.repository.SchemaExporter;

@RestController
@Profile(ProfileNames.HTTP)
public abstract class AbstractStreamController {
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    protected ApplicationModel applicationModel;

    @Autowired
    protected DataSource dataSource;

    private final Map<QualifiedName, Table> cachedForms = new LinkedHashMap<>();

    protected Table lookupTable(QualifiedName qn) {
        // First check cache, then introspect DB
        Table table;
        if (cachedForms.containsKey(qn)) {
            table = cachedForms.get(qn);
        } else {
            table = SchemaExporter.exportTable(dataSource, qn.getSchema(),
                            model -> qn.getTable().equalsIgnoreCase(model.getName()))
                    .orElseThrow(() -> new NotFoundException("No such table: %s".formatted(qn)));
        }
        return table;
    }

    protected void putTable(QualifiedName qn, Table table) {
        cachedForms.put(qn, table);
    }
}
