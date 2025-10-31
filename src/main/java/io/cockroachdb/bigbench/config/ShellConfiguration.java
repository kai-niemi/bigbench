package io.cockroachdb.bigbench.config;

import io.cockroachdb.bigbench.shell.support.AnotherFileValueProvider;
import io.cockroachdb.bigbench.shell.support.FunctionValueProvider;
import io.cockroachdb.bigbench.shell.support.TableNameProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ShellConfiguration {
    @Bean
    public FunctionValueProvider functionValueProvider() {
        return new FunctionValueProvider();
    }

    @Bean
    public TableNameProvider tableNameProvider() {
        return new TableNameProvider();
    }

    @Bean
    public AnotherFileValueProvider anotherFileValueProvider() {
        return new AnotherFileValueProvider();
    }
}
