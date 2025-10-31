package io.cockroachdb.bigbench;

import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import io.cockroachdb.bigbench.model.ImportOption;

@Validated
@ConfigurationProperties(prefix = "model")
public class ApplicationModel {
    private Map<ImportOption, String> csvOptions;

    private Map<ImportOption, String> avroOptions;

    public Map<ImportOption, String> getCsvOptions() {
        return csvOptions;
    }

    public void setCsvOptions(Map<ImportOption, String> csvOptions) {
        this.csvOptions = csvOptions;
    }

    public Map<ImportOption, String> getAvroOptions() {
        return avroOptions;
    }

    public void setAvroOptions(Map<ImportOption, String> avroOptions) {
        this.avroOptions = avroOptions;
    }
}
