package io.cockroachdb.bigbench.model;

public enum ImportOption {
    allow_quoted_null,
    comment,
    data_as_binary_records,
    data_as_json_records,
    decompress,
    delimiter,
    DETACHED,
    fields_enclosed_by,
    fields_escaped_by,
    fields_terminated_by,
    nullif,
    records_terminated_by,
    row_limit,
    rows_terminated_by,
    schema,
    schema_uri,
    skip,
    strict_validation
}
