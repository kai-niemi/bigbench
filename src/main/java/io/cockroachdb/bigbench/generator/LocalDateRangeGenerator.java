package io.cockroachdb.bigbench.generator;

import io.cockroachdb.bigbench.model.Range;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalUnit;
import java.util.Optional;

public class LocalDateRangeGenerator implements ValueGenerator<LocalDate> {
    private final LocalDate startDate;

    private final LocalDate endDate;

    private final long stepAmount;

    private final TemporalUnit stepUnit;

    private LocalDate nextDate;

    public LocalDateRangeGenerator(Range range)
            throws DateTimeParseException {
        this.startDate = range.getFrom() != null ?
                LocalDate.parse(range.getFrom(), DateTimeFormatter.ISO_LOCAL_DATE)
                : LocalDate.now();
        if (Optional.ofNullable(range.getTo()).isPresent()) {
            this.endDate =
                    LocalDate.parse(range.getTo(), DateTimeFormatter.ISO_LOCAL_DATE);
            if (endDate.isBefore(startDate)) {
                throw new IllegalArgumentException("endDate is before startDate");
            }
        } else {
            this.endDate = null;
        }
        this.nextDate = startDate;
        this.stepAmount = range.getStep();
        this.stepUnit = range.getStepUnit();
    }

    @Override
    public LocalDate nextValue() {
        nextDate = nextDate.plus(stepAmount, stepUnit);
        if (endDate != null && nextDate.isAfter(endDate)) {
            nextDate = startDate;
        }
        return nextDate;
    }
}
