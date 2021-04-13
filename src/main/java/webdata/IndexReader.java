package webdata;

import webdata.dictionary.Dictionary;
import webdata.storage.ProductIdToDocIdMapper;
import webdata.storage.ReviewStorage;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.stream.Stream;

public class IndexReader {

	private static final Charset CHARSET = StandardCharsets.ISO_8859_1;
	private final Dictionary dictionary;
	private final ReviewStorage storage;
	private final ProductIdToDocIdMapper prodToDoc;

	/**
	* Creates an IndexReader which will read from the given directory
	*/
	public IndexReader(String dir) {
		try {
			dictionary = new Dictionary(dir, CHARSET, 1024 * 1024 * 50);
			storage = ReviewStorage.inDirectory(dir);
			prodToDoc = new ProductIdToDocIdMapper(dir, storage);
		} catch (IOException ex) {
			throw new RuntimeException("Couldn't read dictionary", ex);
		}
	}
	
	/**
	* Returns the product identifier for the given review
	* Returns null if there is no review with the given identifier
	*/
	public String getProductId(int reviewId) {
		if (reviewId < 1 || reviewId > storage.getNumReviews()) {
			return null;
		}
		return storage.get(reviewId - 1).getProductId();
	}

	/**
	* Returns the score for a given review
	* Returns -1 if there is no review with the given identifier
	*/
	public int getReviewScore(int reviewId) {
		if (reviewId < 1 || reviewId > storage.getNumReviews()) {
			return -1;
		}
		return storage.get(reviewId - 1).getScore();
	}

	/**
	* Returns the numerator for the helpfulness of a given review
	* Returns -1 if there is no review with the given identifier
	*/
	public int getReviewHelpfulnessNumerator(int reviewId) {
		if (reviewId < 1 || reviewId > storage.getNumReviews()) {
			return -1;
		}
		return storage.get(reviewId - 1).getHelpfulnessNumerator();
	}

	/**
	* Returns the denominator for the helpfulness of a given review
	* Returns -1 if there is no review with the given identifier
	*/
	public int getReviewHelpfulnessDenominator(int reviewId) {
		if (reviewId < 1 || reviewId > storage.getNumReviews()) {
			return -1;
		}
		return storage.get(reviewId - 1).getHelpfulnessDenominator();
	}

	/**
	* Returns the number of tokens in a given review
	* Returns -1 if there is no review with the given identifier
	*/
	public int getReviewLength(int reviewId) {

		if (reviewId < 1 || reviewId > storage.getNumReviews()) {
			return -1;
		}
		return storage.get(reviewId - 1).getNumTokens();
	}

	/**
	* Return the number of reviews containing a given token (i.e., word)
	* Returns 0 if there are no reviews containing this token
	*/
	public int getTokenFrequency(String token) {
		int dictIndex = dictionary.getIndexOfToken(token);
		if (dictIndex < 0) {
			return 0;
		}
		return dictionary.getTokenFrequency(dictIndex);
	}

	/**
	* Return the number of times that a given token (i.e., word) appears in
	* the reviews indexed
	* Returns 0 if there are no reviews containing this token
	*/
	public int getTokenCollectionFrequency(String token) {
		int sum = 0;
		var it = getReviewsWithToken(token);
		while (it.hasMoreElements()) {
			int _gap = it.nextElement();
			sum += it.nextElement();
		}
		return sum;
	}

	/**
	* Return a series of integers of the form id-1, freq-1, id-2, freq-2, ... such
	* that id-n is the n-th review containing the given token and freq-n is the
	* number of times that the token appears in review id-n
	* Only return ids of reviews that include the token
	* Note that the integers should be sorted by id
	*
	* Returns an empty Enumeration if there are no reviews containing this token
	*/
	public Enumeration<Integer> getReviewsWithToken(String token) {
		int dictIndex = dictionary.getIndexOfToken(token);
		if (dictIndex < 0) {
			return Utils.streamToEnumeration(Stream.empty());
		}
		try {
			return dictionary.getDocIdsAndFreqs(dictIndex);
		} catch (IOException e) {
			System.err.format("Got IO exception while trying to get reviews with token %s: %s",
					token, e);
			return Utils.streamToEnumeration(Stream.empty());
		}
	}

	/**
	* Return the number of product reviews available in the system
	*/
	public int getNumberOfReviews() {
		return storage.getNumReviews();
	}

	/**
	* Return the number of number of tokens in the system
	* (Tokens should be counted as many times as they appear)
	*/
	public int getTokenSizeOfReviews() {
		return dictionary.getTotalNumberOfTokens();
	}
	
	/**
	* Return the ids of the reviews for a given product identifier
	* Note that the integers returned should be sorted by id
	*
	* Returns an empty Enumeration if there are no reviews for this product
	*/
	public Enumeration<Integer> getProductReviews(String productId) {
		return Utils.streamToEnumeration(prodToDoc.getReviewIdsForProduct(productId).boxed());
	}
}