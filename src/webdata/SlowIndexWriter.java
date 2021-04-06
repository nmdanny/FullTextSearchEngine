package webdata;

import webdata.dictionary.Dictionary;
import webdata.parsing.ParallelReviewParser;

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
		    var dict = new Dictionary(dir);
		    int bufSize = 1 << 16;
		    int numBufs = 4;
			Charset encoding = StandardCharsets.ISO_8859_1;
		    var parser = new ParallelReviewParser(bufSize, numBufs, encoding);

		    parser.parse(inputFile)
					.forEachOrdered(review -> {

					});

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
			var deletedAllFiles = Files.walk(dirPath)
					.sorted(Comparator.reverseOrder())
					.map(Path::toFile)
					.reduce(true, (deleted, file) -> deleted && file.delete(), Boolean::logicalAnd);

			if (!deletedAllFiles) {
				throw new IOException("Couldn't delete all files within directory");
			}
			if (!Files.deleteIfExists(Paths.get(dir))) {
				throw new IOException("Deleted all files within directory, but couldn't delete directory.");
			}
		} catch (IOException ex) {
			System.err.format("Couldn't delete index at %s: %s", dir, ex.toString());
		}
	}
}