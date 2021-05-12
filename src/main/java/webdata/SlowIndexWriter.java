package webdata;

public class SlowIndexWriter {


	private final IndexWriter writer;

	SlowIndexWriter() {
		this.writer = new IndexWriter();
	}
	/**
	 * Given product review data, creates an on disk index
	 * inputFile is the path to the file containing the review data
	 * dir is the directory in which all index files will be created
	 * if the directory does not exist, it should be created
	 */
	public void slowWrite(String inputFile, String dir) {
	    this.writer.write(inputFile, dir);
	}

	/**
	* Delete all index files by removing the given directory
	*/
	public void removeIndex(String dir) {
		this.writer.removeIndex(dir);
	}
}