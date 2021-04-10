package webdata.storage;

import org.junit.jupiter.api.Test;
import webdata.parsing.Review;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class ReviewStorageTest {


    static Review[] REVIEWS = new Review[]{
            Review.fromFields("AAAAAAAAAA", "1/2", "3.0", ""),
            Review.fromFields("ABAAAAAAAA", "3/5", "5.0", ""),
            Review.fromFields("ABAAAAAAAA", "2/5", "4.0", ""),
            Review.fromFields("ABCAAAAAAA", "5/9", "1.0", ""),
            Review.fromFields("ABCAAAAAAA", "5/9", "1.0", ""),
            Review.fromFields("BAAAAAAAAA", "0/1", "2.0", ""),
            Review.fromFields("CAAAAAAAAA", "0/1", "5.0", ""),
            Review.fromFields("DAAAAAAAAA", "4/6", "3.0", "")
    };



    @Test
    void canSaveAndLoadReviews() throws IOException {
        var path = Files.createTempFile("canSaveAndLoadReviews", ".bin");
        path.toFile().deleteOnExit();
        var storage = new ReviewStorage(path);

        storage.appendMany(Arrays.stream(REVIEWS).map(CompactReview::new));

        storage.flush();
        assertEquals(storage.getNumReviews(), REVIEWS.length);

        var expectedCompacts = Arrays.stream(REVIEWS)
                .map(CompactReview::new)
                .collect(Collectors.toList());


        storage = new ReviewStorage(path);
        assertEquals(storage.getNumReviews(), REVIEWS.length);

        var gottenCompacts = new ArrayList<CompactReview>();
        for (int i=1; i <= REVIEWS.length; ++i) {
            var gottenCompact = storage.get(i);
            gottenCompacts.add(gottenCompact);
            assertEquals(REVIEWS[i-1].getScore(), gottenCompact.getScore());
            assertEquals(REVIEWS[i-1].getProductId(), gottenCompact.getProductId());
            assertEquals(REVIEWS[i-1].getHelpfulnessNumerator(), gottenCompact.getHelpfulnessNumerator());
            assertEquals(REVIEWS[i-1].getHelpfulnessDenominator(), gottenCompact.getHelpfulnessDenominator());
            assertEquals(REVIEWS[i-1].getTokens().length, gottenCompact.getNumTokens());
        }
        assertIterableEquals(expectedCompacts, gottenCompacts);
        
        storage.close();
    }

    @Test
    void canBinarySearch() throws IOException {
        var path = Files.createTempFile("canBinarySearch", ".bin");
        try(var storage = new ReviewStorage(path)) {
            storage.appendMany(Arrays.stream(REVIEWS).map(CompactReview::new));

            for (int docId = 1; docId <= REVIEWS.length; ++docId) {
                var expectedReview = new CompactReview(REVIEWS[docId - 1]);
                var gottenReviewRange = storage.binarySearchRange(expectedReview.getProductId());
                assertEquals(2, gottenReviewRange.length);
                assertNotEquals(-1, gottenReviewRange[0]);
                assertNotEquals(-1, gottenReviewRange[1]);
                var lowestReview = storage.get(gottenReviewRange[0]);
                var highestReview = storage.get(gottenReviewRange[1]);
                assertEquals(expectedReview.getProductId(), lowestReview.getProductId());
                assertEquals(expectedReview.getProductId(), highestReview.getProductId());
            }


            assertEquals(0, storage.binarySearchRange("1234567890").length);
        }
    }
}