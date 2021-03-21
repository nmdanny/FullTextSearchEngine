package webdata;

import java.util.HashMap;
import java.util.Objects;

public class Review {
    private String productId;
    private int helpfulnessNumerator;
    private int helpfulnessDenominator;
    private int score;
    private String text;

    public static Review fromFields(HashMap<String, String> fields) {
        String productId = fields.getOrDefault("productId", "");
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

        int scoreInt;
        try {
            scoreInt = (int)Double.parseDouble(score);
            if (scoreInt < 1 || scoreInt > 5)
            {
                throw new IllegalArgumentException("Score " + scoreInt + " is not between 1 and 5");
            }
        } catch (NumberFormatException ex)
        {
            System.err.println("Error parsing score for a review, defaulting to 1: " + ex);
            scoreInt = 1;
        }

        Review review = new Review();
        review.productId = productId;
        review.helpfulnessNumerator = helpfulnessNumerator;
        review.helpfulnessDenominator = helpfulnessDenominator;
        review.score = scoreInt;
        review.text = text;

        return review;
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
