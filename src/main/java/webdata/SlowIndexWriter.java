package webdata;

import webdata.dictionary.InMemoryIndexer;
import webdata.dictionary.SequentialDictionaryBuilder;
import webdata.parsing.ParallelReviewParser;
import webdata.parsing.Review;
import webdata.spimi.SPIMIIndexer;
import webdata.storage.CompactReview;
import webdata.storage.ProductIdToDocIdMapper;
import webdata.storage.ReviewStorage;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

public class SlowIndexWriter {


	public void slowWrite(String inputFile, String dir) {
		try {
			removeIndex(dir);
			Files.createDirectories(Path.of(dir));
			Charset inputFileCharset = StandardCharsets.ISO_8859_1;

			var indexer = new SPIMIIndexer(Path.of(dir));
			try (var storage = ReviewStorage.inDirectory(dir);
				 var mapper = new ProductIdToDocIdMapper(dir, storage)) {

				int bufSize = 1 << 16;
				int numBufs = 4;
				var parser = new ParallelReviewParser(bufSize, numBufs, inputFileCharset);

				final int[] docId = {1};

				var stream = parser.parse(inputFile)
						.sequential()
						.peek(review -> {
							review.assignDocId(docId[0]);
							docId[0] += 1;
							storage.add(new CompactReview(review));
						})
						.flatMap(Review::tokens);
				indexer.processTokens(stream);
			}
		} catch (IOException ex) {

		}
	}

	/**
	* Given product review data, creates an on disk index
	* inputFile is the path to the file containing the review data
	* dir is the directory in which all index files will be created
	* if the directory does not exist, it should be created
	*/
	public void slowWriteSlow(String inputFile, String dir) {
		try {
			removeIndex(dir);
			Files.createDirectories(Path.of(dir));

			Charset inputFileCharset = StandardCharsets.ISO_8859_1;


			try (var seqDictBuilder = new SequentialDictionaryBuilder(dir);
				 var storage = ReviewStorage.inDirectory(dir);
				 var mapper = new ProductIdToDocIdMapper(dir, storage))
			{
				var dictBuilder = new InMemoryIndexer(seqDictBuilder);
				int bufSize = 1 << 16;
				int numBufs = 4;
				var parser = new ParallelReviewParser(bufSize, numBufs, inputFileCharset);

				final int[] docId = {1};

				parser.parse(inputFile)
								.sequential()
								.peek(review -> {
									review.assignDocId(docId[0]);
									dictBuilder.processDocument(review.getDocId(), review.getTokens());
									docId[0] += 1;
									storage.add(new CompactReview(review));
								})
								.sorted(Comparator.comparing(Review::getProductId).thenComparing(Review::getDocId))
								.forEachOrdered(review -> {
									mapper.observeProduct(review.getProductId(), review.getDocId());
								});


				storage.flush();
				dictBuilder.finish();
			}


		} catch (IOException ex) {
			System.err.println("IO exception while creating dictionary: " + ex);
		}
	}

	/**
	* Delete all index files by removing the given directory
	*/
	public void removeIndex(String dir) {
	    try {
	        var dirPath = Paths.get(dir);
	        if (!Files.exists(dirPath)) {
	        	return;
			}
			var deletedAllFiles = Files.walk(dirPath)
					.sorted(Comparator.reverseOrder())
					.map(Path::toFile)
					.reduce(true, (deleted, file) -> deleted && file.delete(), Boolean::logicalAnd);

			if (!deletedAllFiles) {
				System.err.println("Couldn't delete all files within directory\n");
			}
			Files.deleteIfExists(Paths.get(dir));
		} catch (IOException ex) {
			System.err.format("Got exception while trying to delete index at %s: %s\n", dir, ex.toString());
		}
	}
}