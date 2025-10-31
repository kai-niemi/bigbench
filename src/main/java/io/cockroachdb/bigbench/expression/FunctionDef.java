package io.cockroachdb.bigbench.expression;

import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FunctionDef {
    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private final FunctionDef instance = new FunctionDef();

        private Builder() {
        }

        public Builder withArgs(List<String> args) {
            instance.args.addAll(args);
            return this;
        }

        public Builder withId(String id) {
            instance.id = id;
            return this;
        }

        public Builder withCategory(String category) {
            instance.category = category;
            return this;
        }

        public Builder withReturnValue(Class<?> returnType) {
            instance.returnValue = returnType.getSimpleName();
            return this;
        }

        public Builder withReturnValue(String returnValue) {
            instance.returnValue = returnValue;
            return this;
        }

        public Builder withDescription(String description) {
            instance.description = description;
            return this;
        }

        public Builder withFunction(Function function) {
            instance.function = function;
            return this;
        }

        public FunctionDef build() {
            if (!StringUtils.hasLength(instance.id)) {
                throw new IllegalStateException("id is required");
            }
            if (instance.function == null) {
                throw new IllegalStateException("id is required");
            }
            return instance;
        }
    }

    private String id;

    private String category;

    private final List<String> args = new ArrayList<>();

    private String returnValue = "void";

    private String description;

    private Function function;

    private FunctionDef() {
    }

    public String getCategory() {
        return category;
    }

    public String getId() {
        return id;
    }

    public boolean idMatchesPrefix(String prefix) {
        return id.startsWith(prefix);
    }

    public List<String> getArgs() {
        return Collections.unmodifiableList(args);
    }

    public String getReturnValue() {
        return returnValue;
    }

    public String getDescription() {
        return description;
    }

    public Function getFunction() {
        return function;
    }

    public String toSignature() {
        StringBuilder sb = new StringBuilder(getId());
        sb.append("(");
        boolean first = true;
        for (String s : getArgs()) {
            if (!first) {
                sb.append(", ");
            }
            first = false;
            sb.append(s);
        }
        sb.append(") → ");
        sb.append(getReturnValue());
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSignature() + ": " + description;
    }
}
