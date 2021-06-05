package webdata;

import org.junit.jupiter.api.Test;
import webdata.search.SparseVector;

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
    }

    @Test
    void productSearch() {
    }
}