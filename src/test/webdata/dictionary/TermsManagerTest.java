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
        var dir = Path.of("E:\\");
        var manager = new webdata.dictionary.TermsManager(dir.toString(), StandardCharsets.UTF_8);

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
            var charBuf = manager.derefTerm(allocations.get(i));
            assertEquals(strings[i], charBuf.toString());
        }

    }
}
