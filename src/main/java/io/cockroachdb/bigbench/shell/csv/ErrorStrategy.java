package io.cockroachdb.bigbench.shell.csv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;

public enum ErrorStrategy implements ErrorHandler<DataAccessException> {
    IGNORE_AND_CONTINUE {
        @Override
        public void handle(Object source, DataAccessException exception) {
        }
    },
    LOG_AND_CONTINUE {
        @Override
        public void handle(Object source, DataAccessException exception) {
            Logger logger = LoggerFactory.getLogger(source.getClass());
            logger.warn(exception.toString());
        }
    },
    RETHROW {
        @Override
        public void handle(Object source, DataAccessException exception) {
            throw (DataAccessException) exception.fillInStackTrace();
        }
    };
}
