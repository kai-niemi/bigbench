package io.cockroachdb.bigbench.generator;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import io.cockroachdb.bigbench.model.Format;
import io.cockroachdb.bigbench.model.ImportOption;
import io.cockroachdb.bigbench.model.Table;

public class ImportInto {
    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private Format format;

        private Table table;

        private List<String> paths;

        private Map<ImportOption, String> options;

        private Builder() {
        }

        public Builder withFormat(Format format) {
            this.format = format;
            return this;
        }

        public Builder withTable(Table table) {
            this.table = table;
            return this;
        }

        public Builder withPaths(List<String> paths) {
            this.paths = paths;
            return this;
        }

        public Builder withOptions(Map<ImportOption, String> options) {
            this.options = options;
            return this;
        }

        public ImportInto build() {
            Assert.notNull(table, "table is null");

            Assert.notNull(paths, "paths is null");
            Assert.state(!paths.isEmpty(), "paths is empty");

            Assert.notNull(options, "options is null");
            Assert.state(!options.isEmpty(), "options is empty");

            ImportInto importInto = new ImportInto();
            importInto.format = this.format;
            importInto.paths = this.paths;
            importInto.table = this.table;
            importInto.options = this.options;
            return importInto;
        }
    }

    private Format format;

    private Table table;

    private List<String> paths;

    private Map<ImportOption, String> options;

    public String getImportStatement() {
        StringBuilder sb = new StringBuilder();
        sb.append("IMPORT INTO ")
                .append(table.getName())
                .append("(")
                .append(String.join(",", table.columnNames()))
                .append(") ")
                .append(format)
                .append(" DATA (");

        AtomicBoolean first = new AtomicBoolean(true);

        paths.stream()
                .sorted()
                .forEach(path -> {
                    if (!first.get()) {
                        sb.append(", ");
                    }
                    first.set(false);
                    sb.append(" '").append(path).append("'");
                });

        sb.append(")");

        if (!options.isEmpty()) {
            sb.append(" WITH ");

            AtomicBoolean f = new AtomicBoolean(true);

            options.forEach((k, v) -> {
                if (!f.get()) {
                    sb.append(", ");
                }
                sb.append(k);
                if (StringUtils.hasLength(v)) {
                    if (!"(blank)".equalsIgnoreCase(v)) {
                        sb.append(" = '").append(v).append("'");
                    }
                } else if ("".equals(v)) {
                    sb.append(" = ''");
                }
                f.set(false);
            });
        }

        sb.append(";");

        return sb.toString();
    }
}
