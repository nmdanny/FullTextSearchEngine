package webdata.dictionary;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TermsManagerTest {
    @Test
    void canAllocAndDeref() throws IOException {
        var path = Files.createTempFile("termsManagerTest", "small");
        var manager = new webdata.dictionary.TermsManager(path.toString(), StandardCharsets.UTF_8, 1024);

        // technically not terms, but TermManager doesn't really care about the order or contents
        var strings = new String[] {
                "hey",
                "shalom",
                "מה נשמע",
                "cool 🥓🥖🌮😀 אמוג'יים"
        };

        var allocations = Arrays.stream(strings).map(term -> {
            try {
                return manager.allocateTerm(term);
            } catch (IOException ex) {
                System.err.format("Couldn't allocate term for %s: %s", term, ex);
                return null;
            }
        }).collect(Collectors.toList());

        for (int i=0; i < allocations.size(); ++i) {
            var alloc = allocations.get(i);
            var byteBuf = manager.derefTermBytes(alloc.position, alloc.length);
            assertEquals(StandardCharsets.UTF_8.encode(strings[i]), byteBuf);
        }

    }
}
