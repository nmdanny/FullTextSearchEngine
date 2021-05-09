package webdata;

import webdata.search.SparseVector;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class ReviewSearch {
    private final IndexReader reader;

    private final long corpusSize;

    /*** Constructor
     */
    public ReviewSearch(IndexReader iReader) {
        this.reader = iReader;
        this.corpusSize = reader.getNumberOfReviews();
    }


    /** Calculate LTC of the query, that is, cosine-normalized vector of the elementwise product of
     *  logarithmic term frequency with standard inverse document frequency.
     */
    private SparseVector queryLtc(List<String> query) {
        // frequency of each term within the query string
        var naturalTf = query.stream()
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        var tfMap = new HashMap<String, Double>();

        // inverse document frequency within the entire corpus
        var idfMap = new HashMap<String, Double>();

        for (var entry: naturalTf.entrySet()) {
            var term = entry.getKey();
            var tf = entry.getValue();
            assert tf > 0;
            tfMap.put(
                    term,
                    1.0 + Math.log10(tf)
            );

            double dft = reader.getTokenFrequency(term);
            if (dft == 0) {
                continue;
            }
            idfMap.put(
                    term,
                    Math.log10((double)corpusSize / dft)
            );
        }


        return new SparseVector(tfMap).multiply(new SparseVector(idfMap)).cosNormalized();
    }

    /** Calculates the LNN vector(logarithmic term frequency, no IDF/normalization) of each
     *  document which contains any word within the query, returning a mapping between document IDs to
     *  their LNN
     */
    private Map<Integer, SparseVector> docLnns(List<String> query) {
        // maps each docId to a sparse vector representation of its log TF
        var map = new HashMap<Integer, HashMap<String, Double>>();

        for (var term: query) {
            var docPostingIt = reader.getReviewsWithToken(term);
            while (docPostingIt.hasMoreElements()) {
                var docId = docPostingIt.nextElement();
                var freq = docPostingIt.nextElement();
                var sparseVecMap = map.computeIfAbsent(docId, i -> new HashMap<>());
                sparseVecMap.put(term, 1.0 + Math.log10(freq));
            }
        }

        return map.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> new SparseVector(entry.getValue())));
    }

    /**
     * Returns a list of the id-s of the k most highly ranked reviews for the
     * given query, using the vector space ranking function lnn.ltc (using the
     *      SMART notation)
     * The list should be sorted by the ranking
     */
    public Enumeration<Integer> vectorSpaceSearch(Enumeration<String> query, int k) {
        var q = Utils.iteratorToStream(query.asIterator()).collect(Collectors.toList());
        var queryVec = queryLtc(q);
        var docLtcs = docLnns(q);


        return Utils.streamToEnumeration(docLtcs.entrySet()
                    .stream()
                    .sorted(Comparator.comparingDouble(entry -> -queryVec.dot(entry.getValue())))
                    .map(Map.Entry::getKey)
                    .limit(k));
    }

    /**
     * Returns a list of the id-s of the k most highly ranked reviews for the
     * given query, using the language model ranking function, smoothed using a
     * mixture model with the given value of lambda* The list should be sorted by the ranking
     */
    public Enumeration<Integer> languageModelSearch(Enumeration<String> query, double lambda, int k) {

        // maps each doc ID to the product of p(query|doc) using mixture model
        var docToLm = new HashMap<Integer, Double>();

        var totalTokens = reader.getTokenSizeOfReviews();
        while (query.hasMoreElements()) {
            var term = query.nextElement();
            var docPostingIt = reader.getReviewsWithToken(term);

            var cft = reader.getTokenCollectionFrequency(term);
            double probTermInCorpus = (double)cft/totalTokens;

            while (docPostingIt.hasMoreElements()) {
                var docId = docPostingIt.nextElement();
                var tftd = docPostingIt.nextElement();

                var docSize = reader.getReviewLength(docId);
                assert docSize >= tftd;
                double probTermInDoc = (double)tftd/docSize;

                double mix = lambda * probTermInDoc + (1.0 - lambda) * probTermInCorpus;

                // TODO BUG: what if a document is missing some terms? If lambda=1,
                //           then the product should become 0. IF lambda<1, at the very least
                //           should probably decrease. But this loop won't affect that doc's
                //           vector at all!
                docToLm.compute(docId, (_key, lm) -> {
                    if (lm == null) {
                        return mix;
                    }
                    return lm * mix;
                });
            }
        }

        return Utils.streamToEnumeration(docToLm.entrySet()
                .stream()
                .sorted(Comparator.comparingDouble(e -> -e.getValue()))
                .map(Map.Entry::getKey)
                .limit(k));
    }

    /**
     * Returns a list of the id-s of the k most highly ranked productIds for the
     * given query using a function of your choice* The list should be sorted by the ranking
     */
    public Collection<String> productSearch(Enumeration<String> query, int k) {
        return List.of();
    }
}