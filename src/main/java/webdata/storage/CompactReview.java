package webdata.storage;

import webdata.parsing.Review;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/** A compact representation of a review for binary storage */
public class CompactReview implements Comparable<CompactReview> {

    // should be 10 bytes, see https://en.wikipedia.org/wiki/Amazon_Standard_Identification_Number
    private static final int PRODUCT_ID_BYTES = 10;

    private static final Charset STORAGE_ENCODING = StandardCharsets.UTF_8;

    private final byte[] productId;

    // From my observations, a numerator/denominator bigger than the 2^15
    private short helpfulnessDenominator;
    private short helpfulnessNumerator;

    // A score is between 1 and 5
    private byte score;

    // Number of tokens in the review text
    private short numTokens;

    /** Number of bytes taken by a single compact review */
    static final int SIZE_BYTES = PRODUCT_ID_BYTES + 2 + 2 + 1 + 2;

    private CompactReview() {
        this.productId = new byte[PRODUCT_ID_BYTES];
    }

    public CompactReview(Review parsedReview) {
        this.productId = parsedReview.getProductId().getBytes(STORAGE_ENCODING);
        assert (this.productId.length == PRODUCT_ID_BYTES) : String.format("Product '%s' takes more than 10 bytes to encode", parsedReview.getProductId());
        this.helpfulnessNumerator = (short)parsedReview.getHelpfulnessNumerator();
        this.helpfulnessDenominator = (short)parsedReview.getHelpfulnessDenominator();
        this.score = (byte)parsedReview.getScore();
        assert (parsedReview.getTokens().length <= Short.MAX_VALUE) : "Number of reviews in token must be less than 2^15 ";
        this.numTokens = (short)parsedReview.getTokens().length;
    }


    public String getProductId() {
        return new String(productId, STORAGE_ENCODING);
    }

    public short getHelpfulnessDenominator() {
        return helpfulnessDenominator;
    }

    public short getHelpfulnessNumerator() {
        return helpfulnessNumerator;
    }

    public byte getScore() {
        return score;
    }

    public int getNumTokens() { return numTokens; }

    public void serialize(ByteBuffer buf) throws IOException {
        buf.put(productId);
        buf.putShort(helpfulnessNumerator);
        buf.putShort(helpfulnessDenominator);
        buf.put(score);
        buf.putShort(numTokens);
    }

    public void serialize(DataOutputStream os) throws IOException {
        os.write(productId);
        os.writeShort(helpfulnessNumerator);
        os.writeShort(helpfulnessDenominator);
        os.writeByte(score);
        os.writeShort(numTokens);
    }

    public static CompactReview deserialize(ByteBuffer buf) throws IOException {
        var review = new CompactReview();

        buf.get(review.productId);
        review.helpfulnessNumerator = buf.getShort();
        review.helpfulnessDenominator = buf.getShort();
        review.score = buf.get();
        review.numTokens = buf.getShort();
        return review;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CompactReview that = (CompactReview) o;
        return helpfulnessDenominator == that.helpfulnessDenominator && helpfulnessNumerator == that.helpfulnessNumerator && score == that.score && numTokens == that.numTokens && Arrays.equals(productId, that.productId);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(helpfulnessDenominator, helpfulnessNumerator, score, numTokens);
        result = 31 * result + Arrays.hashCode(productId);
        return result;
    }

    @Override
    public String toString() {
        return "CompactReview{" +
                "productId=" + Arrays.toString(productId) +
                ", helpfulnessDenominator=" + helpfulnessDenominator +
                ", helpfulnessNumerator=" + helpfulnessNumerator +
                ", score=" + score +
                ", numTokens=" + numTokens +
                '}';
    }

    @Override
    public int compareTo(CompactReview o) {
        return getProductId().compareToIgnoreCase(o.getProductId());
    }
}
