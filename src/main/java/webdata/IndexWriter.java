package webdata;

import webdata.parsing.LinesMemoryParser;
import webdata.parsing.Review;
import webdata.parsing.SequentialReviewParser;
import webdata.spimi.SPIMIIndexer;
import webdata.storage.CompactReview;
import webdata.storage.ProductIdToDocIdMapper;
import webdata.storage.ReviewStorage;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

public class IndexWriter {

	/**
	 * Given product review data, creates an on disk index
	 * inputFile is the path to the file containing the review data
	 * dir is the directory in which all index files will be created
	 * if the directory does not exist, it should be created
	 */
	public void write(String inputFile, String dir) {
		try {
			Charset inputFileCharset = StandardCharsets.ISO_8859_1;

//          var reviewStream = new SequentialReviewParser(8192, inputFileCharset).parse(inputFile);
			var reviewStream = new LinesMemoryParser().parse(Path.of(inputFile), inputFileCharset);

			writeFromReviews(reviewStream, dir);
		} catch (IOException ex) {
			System.err.println("Got IO exception during slowWrite:\n" + ex);
		}
	}

	public void writeFromReviews(Stream<Review> reviewStream, String dir) throws IOException {
		removeIndex(dir);
		Files.createDirectories(Path.of(dir));

		var indexer = new SPIMIIndexer(Path.of(dir));
		try (var storage = ReviewStorage.inDirectory(dir);
			 var mapper = new ProductIdToDocIdMapper(dir)) {

			final int[] docId = {1};

			var stream = reviewStream
					.sequential()
					.peek(review -> {
						review.assignDocId(docId[0]);
						docId[0] += 1;
						storage.add(new CompactReview(review));
						mapper.observeProduct(review.getProductId(), review.getDocId());
						if (docId[0] % 100000 == 0) {
							Utils.log("== Processed a total of %,d reviews ==", docId[0]);
						}
					})
					.flatMap(Review::uniqueTokens);
			indexer.processTokens(stream);
			mapper.externalSort();
		}
	}

	/**
	 * Delete all index files by removing the given directory
	*/
	public void removeIndex(String dir) {
	    try {
	    	Utils.deleteDirectory(Path.of(dir));
		} catch (IOException ex) {
			System.err.format("Got exception while trying to delete index at %s: %s\n", dir, ex.toString());
		}
	}
}