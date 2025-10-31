package io.cockroachdb.bigbench.shell.support;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.shell.CompletionContext;
import org.springframework.shell.CompletionProposal;
import org.springframework.shell.standard.ValueProvider;

import static java.nio.file.FileVisitOption.FOLLOW_LINKS;

public class AnotherFileValueProvider implements ValueProvider {
    @Override
    public List<CompletionProposal> complete(CompletionContext completionContext) {
        String input = completionContext.currentWordUpToCursor();
        int lastSlash = input.lastIndexOf(File.separatorChar);
        Path dir = lastSlash > -1 ? Paths.get(input.substring(0, lastSlash + 1)) : Paths.get("");
        String prefix = input.substring(lastSlash + 1);

        try {
            return Files
                    .find(dir, 1, (p, a) -> p.getFileName() != null && p.getFileName().toString().startsWith(prefix),
                            FOLLOW_LINKS)
                    .map(p -> {
                        boolean directory = Files.isDirectory(p);
                        String value = p.toString() + (directory ? File.separatorChar : "");
                        return new CompletionProposal(value.replace('\\', '/')) // MingW fix
                                .complete(!directory);
                    })
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
