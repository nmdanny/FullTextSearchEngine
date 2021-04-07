package webdata.dictionary;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DictionaryTest {
    @Test
    void canCreateSmallDictionary() throws IOException {

        var dictFolder = Files.createTempDirectory("canCreateSmallDictionary");
        var dict = new Dictionary(dictFolder.toString(), StandardCharsets.UTF_8, 1024);

        assertEquals(0, dict.stream().count());

        var dictionaryBuilder = new InMemoryDictionaryBuilder(dict);

        dictionaryBuilder.processDocument(1, "שרה שרה שיר שמח שיר שמח שרה שרה test");
        dictionaryBuilder.processDocument(2, "גנן גידל דגן בגן, דגן גדול גדל בגן test");

        dictionaryBuilder.finish();


        assertEquals(10, dict.getUniqueNumberOfTokens());
        assertEquals(18, dict.getTotalNumberOfTokens());

        var elements = dict.stream().collect(Collectors.toList());

        assertEquals(dict.getUniqueNumberOfTokens(), elements.size());
        var expectedElements = new Object[][] {
                {"test", 2},
                {"בגן", 1},
                {"גדול", 1},
                {"גדל", 1},
                {"גידל", 1},
                {"גנן", 1},
                {"דגן", 1},
                {"שיר", 1},
                {"שמח", 1},
                {"שרה", 1},
        };
        for (int i=0; i < expectedElements.length; ++i) {
            Object[] expectedElement = expectedElements[i];
            DictionaryElement gottenElement = elements.get(i);
            assertEquals(expectedElement[0], gottenElement.getTerm().toString());
            assertEquals(expectedElement[1], gottenElement.getTokenFrequency());


        }

        dict.flush();
        dict.close();

    }
}
