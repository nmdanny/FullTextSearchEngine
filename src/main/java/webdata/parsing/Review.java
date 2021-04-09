package webdata.parsing;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Objects;

/** Represents a parsed review */
public class Review {
    private String productId;
    private int helpfulnessNumerator;
    private int helpfulnessDenominator;
    private int score;
    private String text;

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
        review.productId = productId.toLowerCase();
        review.helpfulnessNumerator = helpfulnessNumerator;
        review.helpfulnessDenominator = helpfulnessDenominator;
        review.score = scoreInt;
        review.text = text;

        return review;
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

    public String getText() {
        return text;
    }


    @Override
    public String toString() {
        return "Review{" +
                "productId='" + productId + '\'' +
                ", helpfulnessNumerator=" + helpfulnessNumerator +
                ", helpfulnessDenominator=" + helpfulnessDenominator +
                ", score=" + score +
                ", text='" + text + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Review review = (Review) o;
        return helpfulnessNumerator == review.helpfulnessNumerator && helpfulnessDenominator == review.helpfulnessDenominator && score == review.score && productId.equals(review.productId) && text.equals(review.text);
    }

    @Override
    public int hashCode() {
        return Objects.hash(productId, helpfulnessNumerator, helpfulnessDenominator, score, text);
    }
}
