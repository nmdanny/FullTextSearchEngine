package webdata.dictionary;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

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

        // save dictionary to disk
        dict.close();

        // load dictionary from disk
        dict = new Dictionary(dictFolder.toString(), StandardCharsets.UTF_8, 1024);

        // ensure statistics work
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
            assertEquals(expectedElement[0], dict.getTerm(i).toString());
            assertEquals(expectedElement[1], dict.getTokenFrequency(i));
        }

        // ensure terms are in proper order
        var orderedElements = Arrays.stream(expectedElements)
                .map(arr -> (String)arr[0]).sorted().collect(Collectors.toList());
        Dictionary finalDict = dict;
        var gottenOrdering = IntStream.range(0, dict.getUniqueNumberOfTokens())
                .mapToObj(ix -> finalDict.getTerm(ix).toString()).collect(Collectors.toList());

        assertIterableEquals(orderedElements, gottenOrdering);

        dict.close();

    }
}
