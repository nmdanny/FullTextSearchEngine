package webdata.parsing;

import webdata.Token;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Represents a parsed review */
public class Review {
    // A document ID is set when iterating over parsed documents(as parsing might happen in parallel)
    // and must be over 0
    private int docId = 0;

    private String productId;
    private int helpfulnessNumerator;
    private int helpfulnessDenominator;
    private int score;
    private int totalNumberOfTokens;
    private Map<String, Long> tokenToFreq;

    public static Review fromFields(HashMap<String, String> fields) {
        String productId = fields.getOrDefault("productId", "").toLowerCase();
        String helpfulness = fields.getOrDefault("helpfulness", "0/1");
        String score = fields.getOrDefault("score", "1");
        String text = fields.getOrDefault("text", "");

        return Review.fromFields(
                productId, helpfulness, score, text
        );
    }

    public static Review fromFields(
            String productId,
            String helpfulness,
            String score,
            String text) {
        String[] helpfullnessParts = helpfulness.trim().split("/");
        int helpfulnessNumerator = 0;
        int helpfulnessDenominator = 1;
        try {
            helpfulnessNumerator = Integer.parseInt(helpfullnessParts[0]);
            helpfulnessDenominator = Integer.parseInt(helpfullnessParts[1]);
        } catch (NumberFormatException | IndexOutOfBoundsException ex)
        {
            System.err.println("Error parsing helpfulness for a review, defaulting to 0/1: " + ex);
        }

        // swap numerator/denumerator in case they're flipped
        if (helpfulnessNumerator > helpfulnessDenominator)
        {
            int temp = helpfulnessNumerator;
            helpfulnessNumerator = helpfulnessDenominator;
            helpfulnessDenominator = temp;
        }

        if (helpfulnessNumerator < 0) {
            System.err.println("Helpfulness numerator cannot be negative, changing to 0");
            helpfulnessNumerator = 0;
        }
        if (helpfulnessDenominator < 0) {
            System.err.println("Helpfulness denominator cannot be negative, changing to numerator");
            helpfulnessDenominator = helpfulnessNumerator;
        }

        int scoreInt;
        try {
            scoreInt = (int)Double.parseDouble(score);
            if (scoreInt < 1 || scoreInt > 5)
            {
                throw new NumberFormatException("Score " + scoreInt + " is not between 1 and 5");
            }
        } catch (NumberFormatException ex)
        {
            System.err.println("Error parsing score for a review, defaulting to 1: " + ex);
            scoreInt = 1;
        }

        if (productId.getBytes(StandardCharsets.UTF_8).length != 10) {
            System.err.format("Found a product ID which cannot be encoded in 10 bytes: %s", productId);
        }

        Review review = new Review();
        review.docId = 0;
        review.productId = productId.toLowerCase();
        review.helpfulnessNumerator = helpfulnessNumerator;
        review.helpfulnessDenominator = helpfulnessDenominator;
        review.score = scoreInt;

        int[] totalNumberOfTokens = new int[]{0};
        review.tokenToFreq = Tokenizer.tokensAsStream(text)
                .peek(_term -> totalNumberOfTokens[0] += 1)
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        review.totalNumberOfTokens = totalNumberOfTokens[0];
        return review;
    }

    public int getDocId() {
        if (docId < 1) {
            throw new IllegalStateException("DocID hasn't been assigned yet");
        }
        return this.docId;
    }

    public void assignDocId(int docId) {
        if (this.docId >= 1) {
            throw new IllegalStateException("Can only set document ID once");
        }
        this.docId = docId;
    }

    public String getProductId() {
        return productId;
    }

    public int getHelpfulnessNumerator() {
        return helpfulnessNumerator;
    }

    public int getHelpfulnessDenominator() {
        return helpfulnessDenominator;
    }

    public int getScore() {
        return score;
    }

    public int getTotalNumberOfTokens() { return totalNumberOfTokens; }

    public Stream<Token> uniqueTokens() {
        assert docId >= 0 : "Must be called after docID was set";
        return this.tokenToFreq.entrySet()
                .stream()
                .map(entry -> new Token(entry.getKey(), getDocId(), entry.getValue().intValue()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Review review = (Review) o;
        return docId == review.docId && helpfulnessNumerator == review.helpfulnessNumerator && helpfulnessDenominator == review.helpfulnessDenominator && score == review.score && totalNumberOfTokens == review.totalNumberOfTokens && productId.equals(review.productId) && tokenToFreq.equals(review.tokenToFreq);
    }

    @Override
    public int hashCode() {
        return Objects.hash(docId, productId, helpfulnessNumerator, helpfulnessDenominator, score, totalNumberOfTokens, tokenToFreq);
    }
}
