package webdata.dictionary;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

public class DictionaryTest {

    Charset charset;
    Path tempDir;
    Dictionary dict;
    Object[][] termAndDocumentFreq;


    @BeforeEach
    void setupDictionary() throws IOException {
        charset = StandardCharsets.UTF_8;
        tempDir = Files.createTempDirectory("canCreateSmallDictionary");
        var seqDictBuilder = new SequentialDictionaryBuilder(tempDir.toString(), charset, 1024);

        var dictionaryBuilder = new InMemoryDictionaryBuilder(seqDictBuilder);
        dictionaryBuilder.processDocument(1, "שרה שרה שיר שמח שיר שמח שרה שרה test");
        dictionaryBuilder.processDocument(2, "גנן גידל דגן בגן, דגן גדול גדל בגן test");
        dictionaryBuilder.finish();

        dict = new Dictionary(tempDir.toString(), charset);

        termAndDocumentFreq = new Object[][] {

                {"test", 2}, // FBE
                {"בגן", 1},
                {"גדול", 1},
                {"גדל", 1},
                {"גידל", 1}, // FBE
                {"גנן", 1},
                {"דגן", 1},
                {"שיר", 1},
                {"שמח", 1}, // FBE, err
                {"שרה", 1},
        };
    }

    @Test
    void canGetStatistics() {
        assertEquals(10, dict.getUniqueNumberOfTokens());
        assertEquals(18, dict.getTotalNumberOfTokens());
    }

    @Test
    void canGetDictionaryElements() {
        var elements = dict.stream().collect(Collectors.toList());
        assertEquals(dict.getUniqueNumberOfTokens(), elements.size());
        for (int i = 0; i < termAndDocumentFreq.length; ++i) {
            Object[] expectedElement = termAndDocumentFreq[i];
            assertEquals(expectedElement[0], dict.getTerm(i));
            assertEquals(expectedElement[1], dict.getTokenFrequency(i));
        }

        // ensure terms are in proper order
        var orderedElements = Arrays.stream(termAndDocumentFreq)
                .map(arr -> (String)arr[0]).sorted().collect(Collectors.toList());
        Dictionary finalDict = dict;
        var gottenOrdering = IntStream.range(0, dict.getUniqueNumberOfTokens())
                .mapToObj(finalDict::getTerm).collect(Collectors.toList());

        assertIterableEquals(orderedElements, gottenOrdering);
    }

    @Test
    void canPerformBinarySearchOnDictionary() {
        for (var termAndFreq: termAndDocumentFreq) {
            var term = (String)termAndFreq[0];
            var freq = (int)termAndFreq[1];
            var elementIx = dict.getIndexOfToken(term);
            assertTrue(elementIx >= 0);

            assertEquals(term, dict.getTerm(elementIx));
            assertEquals(freq, dict.getTokenFrequency(elementIx));
        }
    }

    @Test
    void canGetDocIdsAndFreqs() throws IOException {
        // checking a term which appears in two documents
        var testIt  = dict.getDocIdsAndFreqs(dict.getIndexOfToken("test")).asIterator();
        Iterable<Integer> testIterable = () -> testIt;
        assertIterableEquals(List.of(1, 1, 2, 1), testIterable);

        var docIdToDoc = Map.of(
                1, new Object[][] {
                        {"שיר", 2},
                        {"שמח", 2},
                        {"שרה", 4},
                },
                2, new Object[][] {
                        {"גנן",  1},
                        {"גידל",  1},
                        {"דגן",  2},
                        {"בגן",  2},
                        {"גדול",  1},
                        {"גדל",  1},
                }
        );

        // checking terms that only appear in one of the documents
        for (var entries: docIdToDoc.entrySet()) {
            int docId = entries.getKey();
            var doc = entries.getValue();
            for (var termAndFreq: doc) {
                var expectedTerm = (String)termAndFreq[0];
                var expectedFreq = (int)termAndFreq[1];
                var it = dict.getDocIdsAndFreqs(dict.getIndexOfToken(expectedTerm)).asIterator();
                Iterable<Integer> iterable = () -> it;
                assertIterableEquals(List.of(docId, expectedFreq), iterable, "while checking " + termAndFreq[0]);
            }
        }

    }

}
