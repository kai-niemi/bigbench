package io.cockroachdb.bigbench.shell.csv;

import org.springframework.dao.DataAccessException;

public interface ErrorHandler<T extends DataAccessException> {
    void handle(Object source, T exception);
}
