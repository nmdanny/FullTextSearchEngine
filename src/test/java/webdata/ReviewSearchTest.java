package webdata;

import org.junit.jupiter.api.Test;
import webdata.parsing.Review;
import webdata.search.SparseVector;

import java.io.IOException;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;


class ReviewSearchTest {


    @Test
    void testSparseVector() {
        var vec1 = new SparseVector(Map.of("a", 2.0, "b", 2.0));
        var vec2 = new SparseVector(Map.of("a", 2.0, "b", 2.0, "c", 1337.0));
        var dot = vec1.dot(vec2);
        assertEquals(8, dot);
        assertEquals(Math.sqrt(8), vec1.cosNorm());
        assertEquals(Map.of("a", 2.0 / Math.sqrt(8), "b", 2.0 / Math.sqrt(8)), vec1.cosNormalized().elements());

        var multiplied = new SparseVector(Map.of("a", 4.0, "b", 4.0));
        assertEquals(multiplied, vec1.multiply(vec2));
    }

    @Test
    void testLtc() {
        var indexReader = mock(IndexReader.class);

        when(indexReader.getNumberOfReviews()).thenReturn(1000000);
        when(indexReader.getTokenFrequency("best")).thenReturn(50000);
        when(indexReader.getTokenFrequency("car")).thenReturn(10000);
        when(indexReader.getTokenFrequency("insurance")).thenReturn(1000);

        var search = new ReviewSearch(indexReader);
        var result = search.queryLtc(List.of("best", "car", "insurance"));
        var resultRounded = result.elements().entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, ent -> Math.round(ent.getValue() * 100.0) / 100.0));
        assertEquals(Map.of("best", 0.34, "car", 0.52, "insurance", 0.78), resultRounded);
    }

    @Test
    void testLnn() {
        var indexReader = mock(IndexReader.class);

        when(indexReader.getNumberOfReviews()).thenReturn(1000000);

        when(indexReader.getReviewsWithToken("auto")).thenReturn(Utils.streamToEnumeration(Stream.of(9, 1, 1337, 1)));
        when(indexReader.getReviewsWithToken("best")).thenReturn(Utils.streamToEnumeration(Stream.empty()));
        when(indexReader.getReviewsWithToken("car")).thenReturn(Utils.streamToEnumeration(Stream.of(9, 1, 1337, 1)));
        when(indexReader.getReviewsWithToken("insurance")).thenReturn(Utils.streamToEnumeration(Stream.of(9, 2)));

        var search = new ReviewSearch(indexReader);
        var result = search.docLnns(List.of("best", "car", "insurance"));

        /* Theoretically we should have the entry ("auto", 1.0) in the expected LNN vectors for docs 9 and 1337,
           but since that entry would be eliminated when dotting the LNN document vector with the query LTC vector,
           as an optimization I haven't included it in the LNN
         */
        var expected = Map.of(9, new SparseVector(Map.of("car", 1.0, "insurance", 1.0 + Math.log10(2))),
                1337, new SparseVector(Map.of("car", 1.0)));
        assertEquals(result, expected);
    }

    @Test
    void vectorSpaceSearch() {
        var indexReader = mock(IndexReader.class);
        when(indexReader.getNumberOfReviews()).thenReturn(13371337);
        var reviewSearch = spy(new ReviewSearch(indexReader));

        var query = List.of("an", "amazing", "query");
        var queryEnum = Utils.streamToEnumeration(query.stream());


        doReturn(new SparseVector(Map.of(
                "an", 1.0, "amazing", 1.0, "query", 1.0
        ))).when(reviewSearch).queryLtc(query);
        doReturn(Map.of(
                100, new SparseVector(Map.of(
                        "an", 1.0
                )),
                1, new SparseVector(Map.of(
                        "an", 1.0
                )),
                3, new SparseVector(Map.of(
                        "an", 1.0, "amazing", 1.0
                )),
                4, new SparseVector(Map.of(
                        "query", 2.5
                ))
        )).when(reviewSearch).docLnns(query);

        var result = reviewSearch.vectorSpaceSearch(queryEnum, 1000);
        var resultList = Utils.iteratorToStream(result.asIterator()).collect(Collectors.toList()) ;

        assertIterableEquals(List.of(4, 3, 1, 100), resultList);
    }

    @Test
    void languageModelSearch() {
        var query = List.of("enjoy", "your", "vacation");
        var queryEnum = Utils.streamToEnumeration(query.stream());

        var indexReader = mock(IndexReader.class);

        when(indexReader.getTokenSizeOfReviews()).thenReturn(5000000);

        when(indexReader.getTokenCollectionFrequency("vacation")).thenReturn(100000);
        when(indexReader.getTokenCollectionFrequency("enjoy")).thenReturn(200000);
        when(indexReader.getTokenCollectionFrequency("your")).thenReturn(500000);
        when(indexReader.getTokenCollectionFrequency("very")).thenReturn(500000);
        when(indexReader.getTokenCollectionFrequency("friend")).thenReturn(500000);


        // assume we have a document with docID 3:
        // "enjoy enjoy enjoy enjoy vacation vacation very very your friend"
        // which means we have the tokens:
        // (enjoy,4) (vacation,2) (very, 2) (your,1) (friend,1)

        when(indexReader.getReviewLength(3)).thenReturn(10);
        when(indexReader.getReviewsWithToken("enjoy")).thenAnswer(_unused -> Utils.streamToEnumeration(
                Stream.of(3, 4)
        ));
        when(indexReader.getReviewsWithToken("vacation")).thenAnswer(_unused -> Utils.streamToEnumeration(
                Stream.of(3, 2)
        ));
        when(indexReader.getReviewsWithToken("very")).thenAnswer(_unused -> Utils.streamToEnumeration(
                Stream.of(3, 2)
        ));
        when(indexReader.getReviewsWithToken("your")).thenAnswer(_unused -> Utils.streamToEnumeration(
                Stream.of(3, 1)
        ));
        when(indexReader.getReviewsWithToken("friend")).thenAnswer(_unused -> Utils.streamToEnumeration(
                Stream.of(3, 1)
        ));


        var search = new ReviewSearch(indexReader);
        var scores = search.getLanguageModelScores(new HashSet<>(query), 0.5);
        for (var kvp: scores.entrySet()) {
            var rounded = Math.round(kvp.getValue() * 1e5)/1e5;
            kvp.setValue(rounded);
        }

        var expected = Map.of(
                3, 0.00242
        );

        assertEquals(expected, scores);


        var result = search.languageModelSearch(queryEnum, 0.5, 1000);
        var resultList = Utils.iteratorToStream(result.asIterator()).collect(Collectors.toList()) ;

        assertIterableEquals(List.of(3), resultList);

    }

    @Test
    void productSearch() throws IOException {
        var tmpDir = Files.createTempDirectory("productSearch");

        var reviewStream = Stream.of(
                Review.fromFields(Map.of(
                        "productId", "12345ABCDE",
                        "helpfulness", "1/10",
                        "score", "5",
                        "text", "this is good phone"
                )),
                Review.fromFields(Map.of(
                        "productId", "12345ABCDE",
                        "helpfulness", "1/10",
                        "score", "5",
                        "text", "this phone is amazing"
                )),
                Review.fromFields(Map.of(
                        "productId", "12345ABCDE",
                        "helpfulness", "83/100",
                        "score", "1",
                        "text", "this phone is terrible and has many glitches."
                )),
                Review.fromFields(Map.of(
                        "productId", "1234567890",
                        "helpfulness", "93/100",
                        "score", "5",
                        "text", "this phone is OK"
                ))
        );


        new IndexWriter().writeFromReviews(reviewStream, tmpDir.toString());
        var indexReader = new IndexReader(tmpDir.toString());
        var search = new ReviewSearch(indexReader);

        var results = search.productSearch(Utils.streamToEnumeration(
                Stream.of("phone")
        ), 100);

        assertIterableEquals(List.of("1234567890", "12345ABCDE"), results);
    }

    /** test for edge-case where the index is empty */
    @Test
    void operationsEmptyIndex() throws IOException {

        var tmpDir = Files.createTempDirectory("operationsEmptyIndex");

        Stream<Review> reviewStream = Stream.empty();
        new IndexWriter().writeFromReviews(reviewStream, tmpDir.toString());
        var indexReader = new IndexReader(tmpDir.toString());
        var search = new ReviewSearch(indexReader);

        var prods = search.productSearch(Utils.streamToEnumeration(Stream.empty()), 10);
        var prods2 = search.productSearch(Utils.streamToEnumeration(Stream.of("a", "b")), 10);
        var prods3 = search.vectorSpaceSearch(Utils.streamToEnumeration(Stream.of("a", "b")), 10);
        var prods4 = search.languageModelSearch(Utils.streamToEnumeration(Stream.of("a", "b")), 0.5, 10);

        assertTrue(prods.isEmpty());
        assertTrue(prods2.isEmpty());
        assertFalse(prods3.hasMoreElements());
        assertFalse(prods4.hasMoreElements());

    }
}