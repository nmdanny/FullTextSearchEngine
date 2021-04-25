package webdata;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.GsonBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

class Scenario {
    Path tempDir;
    Path  inputPath;
    IndexReader reader;
    ExpectedResults expectedObject;

    @Override
    public String toString() {
        return "Scenario for " + inputPath.getFileName();
    }
}

/** Expected results (using lowercase strings) */
class ExpectedResults {
    int totalTokens;
    int numReviews;
    TreeMap<String, ExpectedEntry[]> productIdToEntry;
    TreeMap<String, int[]> termToPostings;
    TreeMap<String, Integer> termToCollectionFrequency;
    TreeMap<String, int[]> productIdToReviewIds;
}

/** An expected review entry (using lowercase strings)*/
class ExpectedEntry {
    String productId;
    int docId;
    int helpfulnessNumerator;
    int helpfulnessDenominator;
    int score;
    String[] tokens;
}

public class IndexReaderIntegrationTest {

    // relative to project root (sibling of test folder)
    static final Path DATASETS_AND_JSONS_PATH = Path.of("datasets");

    static Stream<Scenario> scenarioStream() {
       return Stream.of(
               DATASETS_AND_JSONS_PATH.resolve("100.txt"),
               DATASETS_AND_JSONS_PATH.resolve("1000.txt")
       ).map(txtPath -> {
           try {
               return loadScenario(txtPath);
           } catch (IOException e) {
               throw new RuntimeException("Couldn't load scenario", e);
           }
       });
    }

    static Map<Path, Scenario> scenarioCache = new HashMap<>();

    /** Creates a test scenario
     * @param txtPath Path of .txt input file, there should also be a sibling .json file of the same name
     */
    static Scenario loadScenario(Path txtPath) throws IOException {

        if (scenarioCache.containsKey(txtPath)) {
            return scenarioCache.get(txtPath);
        }

        var fileNameNoExt  = txtPath.getFileName().toString().replace(".txt", "");
        var tempDir = Files.createTempDirectory("IndexReaderTest-" + fileNameNoExt + "-");

        var scen = new Scenario();
        scen.tempDir = tempDir;
        scen.inputPath = txtPath;

        var jsonPath = Path.of(txtPath.toString().replace(".txt", ".json"));

        var gson = new GsonBuilder()
                .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
                .create();
        var reader = Files.newBufferedReader(jsonPath);

        new SlowIndexWriter().slowWrite(txtPath.toString(), tempDir.toString());
        scen.reader = new IndexReader(tempDir.toString());
        scen.expectedObject = gson.fromJson(reader, ExpectedResults.class);

        scenarioCache.put(txtPath, scen);
        return scen;
    }

    static <T> List<T> enumerationToList(Enumeration<T> enumeration) {
        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(enumeration.asIterator(), Spliterator.ORDERED), false
        ).collect(Collectors.toList());
    }

    @ParameterizedTest
    @MethodSource("scenarioStream")
    void canGetReviewDataFromDocID(Scenario scenario) {
        var reader = scenario.reader;
        var expected = scenario.expectedObject;

        for (var productIdAndEntry : expected.productIdToEntry.entrySet()) {
            var expectedEntries = productIdAndEntry.getValue();

            for (var entry: expectedEntries) {
                assertEquals(entry.productId, reader.getProductId(entry.docId));
                assertEquals(entry.score, reader.getReviewScore(entry.docId));
                assertEquals(entry.helpfulnessNumerator, reader.getReviewHelpfulnessNumerator(entry.docId));
                assertEquals(entry.helpfulnessDenominator, reader.getReviewHelpfulnessDenominator(entry.docId));
                assertEquals(entry.tokens.length, reader.getReviewLength(entry.docId));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("scenarioStream")
    void getProductReviews(Scenario scenario) {
        var reader = scenario.reader;
        var expected = scenario.expectedObject;
        for (var entry: expected.productIdToReviewIds.entrySet()) {
            var productId = entry.getKey();
            var expectedDocIDs = entry.getValue();

            Iterable<Integer> expectedReviewsContaining = () -> IntStream.of(expectedDocIDs).boxed().iterator();
            assertIterableEquals(
                    expectedReviewsContaining,
                    enumerationToList(reader.getProductReviews(productId)),
                    "Mismatch between expected and gotten review IDs given product ID " + productId
            );
        }
    }

    @ParameterizedTest
    @MethodSource("scenarioStream")
    void getReviewsWithToken(Scenario scenario) {
        var reader = scenario.reader;
        var expected = scenario.expectedObject;

       for (var tokenAndPosting: expected.termToPostings.entrySet()) {
           var token = tokenAndPosting.getKey();

           int gottenTokenFrequency = reader.getTokenFrequency(token);

           int[] gottenTokensAndFreqs = new int[gottenTokenFrequency * 2];

           var gottenIt = reader.getReviewsWithToken(token).asIterator();
           for (int i=0; i < gottenTokensAndFreqs.length; ++i) {
               assertTrue(gottenIt.hasNext());
               gottenTokensAndFreqs[i] = gottenIt.next();
           }
           assertFalse(gottenIt.hasNext());
           assertArrayEquals(tokenAndPosting.getValue(), gottenTokensAndFreqs);

           int gottenTokenCollectionFrequency = reader.getTokenCollectionFrequency(token);
           assertEquals(expected.termToCollectionFrequency.get(token), gottenTokenCollectionFrequency);
       }


    }

    @ParameterizedTest
    @MethodSource("scenarioStream")
    void getNumberOfReviewsAndTokenSizeOfReviews(Scenario scenario) {
        var reader = scenario.reader;
        var expected = scenario.expectedObject;

        assertEquals(expected.numReviews, reader.getNumberOfReviews());
        assertEquals(expected.totalTokens, reader.getTokenSizeOfReviews());
    }

    @BeforeAll
    static void deleteDictionaries() {
        for (var scenario: scenarioCache.values()) {
            new SlowIndexWriter().removeIndex(scenario.tempDir.toString());
        }
    }
}
