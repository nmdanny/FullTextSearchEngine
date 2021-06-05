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
     *  their LNN. The returned vector only contains entries(log TFs) of terms that appear in the query, and not
     *  all terms of said documents.
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
                // compare by ranks in descending order, break ties by review IDs in ascending order.
                .sorted(Comparator.comparingDouble(
                        (Map.Entry<Integer, SparseVector> entry)-> queryVec.dot(entry.getValue())).reversed()
                        .thenComparingInt(Map.Entry::getKey)
                )
                .map(Map.Entry::getKey)
                .limit(k));
    }

    /**
     * Returns a list of the id-s of the k most highly ranked reviews for the
     * given query, using the language model ranking function, smoothed using a
     * mixture model with the given value of lambda
     * The list should be sorted by the ranking
     */
    public Enumeration<Integer> languageModelSearch(Enumeration<String> query, double lambda, int k) {

        var queryWords = Utils.iteratorToStream(query.asIterator()).collect(Collectors.toList());

        var totalTokens = reader.getTokenSizeOfReviews();
        // maps each query term `t` to cf_t, that is, corpus frequency of `t`
        var termToCorpusFreq = queryWords.stream()
                .collect(Collectors.toMap(
                        Function.identity(), reader::getTokenCollectionFrequency
                ));

        // maps each query term to a map between documents which contain it, and frequency of said term within each
        // of those documents
        var termToDocToFreq = queryWords.stream()
                .collect(Collectors.toMap(
                        Function.identity(), term -> {
                            var map = new HashMap<Integer, Integer>();
                            var it = reader.getReviewsWithToken(term);
                            while (it.hasMoreElements()) {
                                var docId = it.nextElement();
                                var freq = it.nextElement();
                                var prev = map.put(docId, freq);
                                assert prev == null;
                            }
                            return map;
                        }
                ));

        // find all docIDs that contain at least 1 of the query words - these are deemed candidates.
        var involvedDocs = termToDocToFreq.values().stream()
                .flatMap(m -> m.keySet().stream())
                .collect(Collectors.toSet());

        // calculate p(query|doc) for each document using mixture model
        var docToScore = involvedDocs
                .stream()
                .collect(Collectors.toMap(Function.identity(), docId -> {
                    double score = 0.0;
                    int docSize = reader.getReviewLength(docId);
                    for (var term : queryWords) {
                        // p(term|document) = tftd/docSize
                        double mleTerm = (double)termToDocToFreq.get(term).getOrDefault(docId, 0) / docSize;

                        // p(term|corpus) = corpus_ft/corpusSize
                        double smoothTerm = (double)termToCorpusFreq.get(term) / totalTokens;
                        score += lambda * mleTerm + (1.0 - lambda) * smoothTerm;
                    }
                    return score;
                }));

        // return an enumeration of the 'k' docIds with the highest score
        return Utils.streamToEnumeration(docToScore.entrySet()
                .stream()
                .sorted(Map.Entry.<Integer, Double>comparingByValue().reversed()
                        .thenComparingInt(Map.Entry::getKey))
                .map(Map.Entry::getKey)
                .limit(k)
        );
    }

    /**
     * Returns a list of the id-s of the k most highly ranked productIds for the
     * given query using a function of your choice* The list should be sorted by the ranking
     */
    public Collection<String> productSearch(Enumeration<String> query, int k) {
        return List.of();
    }
}