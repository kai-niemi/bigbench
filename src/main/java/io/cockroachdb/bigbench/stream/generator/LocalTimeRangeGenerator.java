package io.cockroachdb.bigbench.stream.generator;

import io.cockroachdb.bigbench.model.Range;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalUnit;
import java.util.Optional;

public class LocalTimeRangeGenerator implements ValueGenerator<LocalTime> {
    private final LocalTime startTime;

    private final LocalTime endTime;

    private final long stepAmount;

    private final TemporalUnit stepUnit;

    private LocalTime nextTime;

    public LocalTimeRangeGenerator(Range range)
            throws DateTimeParseException {
        this.startTime = range.getFrom() != null ?
                LocalTime.parse(range.getFrom(), DateTimeFormatter.ISO_LOCAL_TIME)
                : LocalTime.now();
        if (Optional.ofNullable(range.getTo()).isPresent()) {
            this.endTime =
                    LocalTime.parse(range.getTo(), DateTimeFormatter.ISO_LOCAL_TIME);
            if (endTime.isBefore(startTime)) {
                throw new IllegalArgumentException("endTime is before startTime");
            }
        } else {
            this.endTime = null;
        }
        this.nextTime = startTime;
        this.stepAmount = range.getStep();
        this.stepUnit = range.getStepUnit();
    }

    @Override
    public LocalTime nextValue() {
        nextTime = nextTime.plus(stepAmount, stepUnit);
        if (endTime != null) {
            if (nextTime.isAfter(endTime)) {
                nextTime = startTime;
            }
        }
        return nextTime;
    }
}
