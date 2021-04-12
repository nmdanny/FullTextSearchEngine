package webdata;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.GsonBuilder;
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
}

class ExpectedResults {
    int totalTokens;
    int uniqueTokens;
    int numReviews;
    TreeMap<String, Entry> productIdToEntry;
    TreeMap<String, int[]> termToPostings;
    TreeMap<String, Integer> termToCollectionFrequency;
    TreeMap<String, int[]> productIdToReviewIds;
}

class Entry {
    String productId;
    int docId;
    int helpfulnessNumerator;
    int helpfulnessDenominator;
    int score;
    String[] tokens;
}


public class IndexReaderIntegrationTest {


    static Stream<Scenario> scenarioStream() {
       return Stream.of(
               Path.of("datasets", "100.txt"),
               Path.of("datasets", "1000.txt")
       ).map(txtPath -> {
           try {
               return loadScenario(txtPath);
           } catch (IOException e) {
               throw new RuntimeException("Couldn't load scenario", e);
           }
       });
    }

    static Scenario loadScenario(Path txtPath) throws IOException {

        var fileNameNoExt  = txtPath.getFileName().toString().replace(".txt", "");
        var tempDir = Files.createTempDirectory("IndexReaderTest-" + fileNameNoExt);

        var scen = new Scenario();
        scen.tempDir = tempDir;
        scen.inputPath = txtPath;

        var json = txtPath.toString().replace(".txt", ".json");

        var gson = new GsonBuilder()
                .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
                .create();
        var reader = Files.newBufferedReader(Path.of(json));

        new SlowIndexWriter().slowWrite(txtPath.toString(), tempDir.toString());
        scen.reader = new IndexReader(tempDir.toString());
        scen.expectedObject = gson.fromJson(reader, ExpectedResults.class);
        return scen;
    }

    static <T> List<T> enumerationToList(Enumeration<T> enumeration) {
        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(enumeration.asIterator(), Spliterator.ORDERED), false
        ).collect(Collectors.toList());
    }

    @ParameterizedTest
    @MethodSource("scenarioStream")
    void testScenario(Scenario scenario) {
        var reader = scenario.reader;
        var expected = scenario.expectedObject;

       for (var productIdAndEntry : expected.productIdToEntry.entrySet()) {
           var entry = productIdAndEntry.getValue();

           assertEquals(entry.productId, reader.getProductId(entry.docId));
           assertEquals(entry.score, reader.getReviewScore(entry.docId));
           assertEquals(entry.helpfulnessNumerator, reader.getReviewHelpfulnessNumerator(entry.docId));
           assertEquals(entry.helpfulnessDenominator, reader.getReviewHelpfulnessDenominator(entry.docId));
           assertEquals(entry.tokens.length, reader.getReviewLength(entry.docId));

           // TODO: fix everything regarding doid ordering
           Iterable<Integer> expectedReviewsContaining = () -> IntStream.of(expected.productIdToReviewIds.get(entry.productId)).boxed().iterator();
//           assertIterableEquals(
//                   expectedReviewsContaining,
//                   enumerationToList(reader.getProductReviews(entry.productId))
//           );
       }

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

        assertEquals(expected.numReviews, reader.getNumberOfReviews());
        assertEquals(expected.totalTokens, reader.getTokenSizeOfReviews());

    }

}
