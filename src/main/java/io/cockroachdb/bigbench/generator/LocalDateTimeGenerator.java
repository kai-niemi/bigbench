package io.cockroachdb.bigbench.generator;

import io.cockroachdb.bigbench.model.Range;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalUnit;
import java.util.Optional;

public class LocalDateTimeGenerator implements ValueGenerator<LocalDateTime> {
    private final LocalDateTime startTime;

    private final LocalDateTime endTime;

    private final long stepAmount;

    private final TemporalUnit stepUnit;

    private LocalDateTime nextTime;

    public LocalDateTimeGenerator(Range range)
            throws DateTimeParseException {
        this.startTime = range.getFrom() != null ?
                LocalDateTime.parse(range.getFrom(), DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                : LocalDateTime.now();
        if (Optional.ofNullable(range.getTo()).isPresent()) {
            this.endTime =
                    LocalDateTime.parse(range.getTo(), DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            if (endTime.isBefore(startTime)) {
                throw new IllegalArgumentException("endDateTime is before startDateTime");
            }
        } else {
            this.endTime = null;
        }
        this.nextTime = startTime;
        this.stepAmount = range.getStep();
        this.stepUnit = range.getStepUnit();
    }

    @Override
    public LocalDateTime nextValue() {
        nextTime = nextTime.plus(stepAmount, stepUnit);
        if (endTime != null && nextTime.isAfter(endTime)) {
            nextTime = startTime;
        }
        return nextTime;
    }
}

