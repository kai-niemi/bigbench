package io.cockroachdb.bigbench.web;

public abstract class LinkRelations {
    public static final String ACTUATORS_REL = "actuators";

    public static final String SCHEMA_INDEX_REL = "schemas";

    public static final String CSV_STREAM_REL = "csv-stream";

    public static final String CSV_FORM_REL = "csv-form";

    public static final String CSV_IMPORT_REL = "csv-import";

    public static final String AVRO_STREAM_REL = "avro-stream";

    public static final String AVRO_FORM_REL = "avro-form";

    public static final String AVRO_IMPORT_REL = "avro-import";

    // IANA standard link relations:
    // http://www.iana.org/assignments/link-relations/link-relations.xhtml

    public static final String CURIE_NAMESPACE = "bigbench";

    private LinkRelations() {
    }

}
