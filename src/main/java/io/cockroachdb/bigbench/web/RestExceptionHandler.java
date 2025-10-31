package io.cockroachdb.bigbench.web;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;

import org.springframework.context.annotation.Profile;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import jakarta.servlet.RequestDispatcher;
import jakarta.servlet.http.HttpServletRequest;

import io.cockroachdb.bigbench.config.ProfileNames;

@RestControllerAdvice
@Profile(ProfileNames.HTTP)
public class RestExceptionHandler extends ResponseEntityExceptionHandler {
    @RequestMapping("/error")
    public ProblemDetail handleError(HttpServletRequest request) {
        Object status = request.getAttribute(RequestDispatcher.ERROR_STATUS_CODE);
        HttpStatus httpStatus;
        if (status != null) {
            httpStatus = HttpStatus.valueOf(Integer.parseInt(status.toString()));
        } else {
            httpStatus = HttpStatus.INTERNAL_SERVER_ERROR;
        }
        return ProblemDetail.forStatus(httpStatus);
    }

    @ExceptionHandler({Exception.class})
    public ResponseEntity<ProblemDetail> handleAny(Throwable ex) {
        if (ex instanceof UndeclaredThrowableException) {
            ex = ((UndeclaredThrowableException) ex).getUndeclaredThrowable();
        }
        ResponseStatus responseStatus = AnnotationUtils.findAnnotation(ex.getClass(), ResponseStatus.class);
        HttpStatus status = responseStatus != null ? responseStatus.value() : HttpStatus.INTERNAL_SERVER_ERROR;
        return toEntity(status, "Uncategorized error", ex);
    }

    @ExceptionHandler(value = {FileNotFoundException.class})
    public ResponseEntity<ProblemDetail> handleFileNotFoundException(FileNotFoundException ex) {
        return toEntity(HttpStatus.NOT_FOUND, "File not found", ex);
    }

    @ExceptionHandler(value = {NotFoundException.class})
    public ResponseEntity<ProblemDetail> handleFileNotFoundException(NotFoundException ex) {
        return toEntity(HttpStatus.NOT_FOUND, "Resource not found", ex);
    }

    @ExceptionHandler(IOException.class)
    public ResponseEntity<ProblemDetail> handleIOException(IOException e) {
        return toEntity(HttpStatus.INTERNAL_SERVER_ERROR, "I/O error", e);
    }

    private ResponseEntity<ProblemDetail> toEntity(HttpStatus status, String title, Throwable ex) {
        ProblemDetail problemDetail = ProblemDetail.forStatusAndDetail(status, ex.getMessage());
        problemDetail.setTitle(title);
        if (status.is5xxServerError()) {
            logger.error("Server error processing request", ex);
        } else if (status.is4xxClientError()) {
            logger.warn("Client error processing request", ex);
        }
        return ResponseEntity
                .status(status)
                .contentType(MediaType.APPLICATION_PROBLEM_JSON)
                .body(problemDetail);
    }
}
