package io.cockroachdb.bigbench.model;

import java.time.temporal.ChronoUnit;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Range {
    private RangeType type;

    private String from;

    private String to;

    private Integer step;

    private ChronoUnit stepUnit;

    public RangeType getType() {
        return type;
    }

    public void setType(RangeType type) {
        this.type = type;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public String getTo() {
        return to;
    }

    public void setTo(String to) {
        this.to = to;
    }

    public Integer getStep() {
        return step;
    }

    public void setStep(Integer step) {
        this.step = step;
    }

    public ChronoUnit getStepUnit() {
        return stepUnit;
    }

    public void setStepUnit(ChronoUnit stepUnit) {
        this.stepUnit = stepUnit;
    }

    @Override
    public String toString() {
        return "Range{" +
                "from='" + from + '\'' +
                ", type=" + type +
                ", to='" + to + '\'' +
                ", step=" + step +
                ", stepUnit=" + stepUnit +
                '}';
    }
}
