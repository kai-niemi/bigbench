package io.cockroachdb.bigbench.web;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
import io.cockroachdb.bigbench.jdbc.SchemaExporter;

@RestController
@Profile(ProfileNames.HTTP)
public abstract class AbstractStreamController {
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final Map<QualifiedName, Table> tableFormCache = new ConcurrentHashMap<>();

    @Autowired
    protected ApplicationModel applicationModel;

    @Autowired
    protected DataSource dataSource;

    protected Table lookupTable(QualifiedName qn) {
        Table table;
        if (!tableFormCache.containsKey(qn)) {
            table = SchemaExporter.exportTable(dataSource, qn.getSchema(),
                            model -> qn.getTable().equalsIgnoreCase(model.getName()))
                    .orElseThrow(() -> new NotFoundException("No such table: %s".formatted(qn)));
            putTable(qn, table);
        } else {
            table = tableFormCache.get(qn);
        }
        return table;
    }

    protected void putTable(QualifiedName qn, Table table) {
        tableFormCache.put(qn, table);
    }
}
