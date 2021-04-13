package webdata;

import webdata.dictionary.InMemoryDictionaryBuilder;
import webdata.dictionary.SequentialDictionaryBuilder;
import webdata.parsing.ParallelReviewParser;
import webdata.parsing.Review;
import webdata.storage.CompactReview;
import webdata.storage.ReviewStorage;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

public class SlowIndexWriter {

	/**
	* Given product review data, creates an on disk index
	* inputFile is the path to the file containing the review data
	* dir is the directory in which all index files will be created
	* if the directory does not exist, it should be created
	*/
	public void slowWrite(String inputFile, String dir) {
		try {
			removeIndex(dir);
			Files.createDirectories(Path.of(dir));

			int mmapSize = (int)Long.min(Integer.MAX_VALUE, Files.size(Path.of(inputFile)));
			Charset charset = StandardCharsets.ISO_8859_1;


			try (var seqDictBuilder = new SequentialDictionaryBuilder(dir, charset, mmapSize);
				 var storage = ReviewStorage.inDirectory(dir))
			{
				var dictBuilder = new InMemoryDictionaryBuilder(seqDictBuilder);
				int bufSize = 1 << 16;
				int numBufs = 4;
				var parser = new ParallelReviewParser(bufSize, numBufs, charset);

				final int[] docId = {1};
				parser.parse(inputFile)
						.forEachOrdered(review -> {
							review.assignDocId(docId[0]);
							dictBuilder.processDocument(review.getDocId(), review.getTokens());
							docId[0] += 1;
							storage.add(new CompactReview(review));
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
			if (!Files.deleteIfExists(Paths.get(dir))) {
				System.err.println("Deleted all files within directory, but couldn't delete directory.\n");
			}
		} catch (IOException ex) {
			System.err.format("Got exception while trying to delete index at %s: %s\n", dir, ex.toString());
		}
	}
}