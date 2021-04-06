package webdata.dictionary;

import webdata.PostingListWriter;

import java.io.*;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;

/** The dictionary is held within memory in a compressed form
 *
 */
public class Dictionary implements Closeable, Flushable {

    private final String dir;

    // Contains the memory blocks
    private final ArrayList<DictionaryBlock> blocks;
    private final PostingListWriter postingListWriter;


    // Contains the terms
    private static final String TERMS_FILE_NAME = "terms.bin";
    private final RandomAccessFile termsFile;
    private final FileChannel termsFileChannel;
    // TODO: doesn't support having strings take more than ~2gb
    private final MappedByteBuffer termsBuf;

    // Contains the postings
    private static final String POSTINGS_FILE_NAME = "postings.bin";


    private static final Charset ENCODING = StandardCharsets.ISO_8859_1;
    private static final int BLOCK_SIZE = 4;

    private int totalNumberOfTokens;

    /**
     * Loads a dictionary(if directory exists), otherwise creates one
     * @param dir Directory
     * @throws IOException
     */
    public Dictionary(String dir) throws IOException {
        this.dir = dir;

        if (dir.contains(TERMS_FILE_NAME) && dir.contains(POSTINGS_FILE_NAME)) {
            throw new RuntimeException("TODO Load dictionary");
        } else {
            Files.createDirectories(Paths.get(dir));

            var postingsFos = new FileOutputStream(Paths.get(dir, POSTINGS_FILE_NAME).toString());
            var postingsOs = new BufferedOutputStream(postingsFos);
            this.postingListWriter = new PostingListWriter(postingsFos, postingsOs);

            this.blocks = new ArrayList<>();
            this.totalNumberOfTokens = 0;


            termsFile = new RandomAccessFile(Paths.get(dir, TERMS_FILE_NAME).toString(), "rw");
            termsFileChannel = termsFile.getChannel();
            this.termsBuf = termsFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, Integer.MAX_VALUE);
        }
    }

    /** Given a term pointer and length, returns the term. */
    public CharBuffer derefTermPointer(int termPointer, int termLength) {
        var bytes = this.termsBuf.slice()
                .position(termPointer)
                .limit(termPointer + termLength);
        return ENCODING.decode(bytes);
    }

    /** Begins a new term */
    public void beginTerm(String term) throws IOException {
        if (postingListWriter.getCurrentTerm() != null && postingListWriter.getCurrentTerm().compareTo(term) >= 0)
        {
            throw new IllegalArgumentException("Terms must be written into dictionary in lexicographically increasing order");
        }
        // If this isn't the first term
        if (totalNumberOfTokens > 0) {
            endTerm();
        }
        postingListWriter.startTerm(term);
    }

    public void endTerm() throws IOException {

    }

    public void addTerm(int docId, String[] reviewTerms) throws IOException {
        String term = postingListWriter.getCurrentTerm();
        long freqInDoc = Arrays.stream(reviewTerms)
                              .filter(someTerm -> someTerm.equals(term))
                              .count();
        addTermOccurence(docId, (int)freqInDoc);

    }

    public void addTermOccurence(int docId, int freqInDoc) throws IOException {
        postingListWriter.add(docId, freqInDoc);
    }

    @Override
    public void close() throws IOException {
        postingListWriter.close();
        termsFileChannel.close();
        termsFile.close();
    }

    @Override
    public void flush() throws IOException {
        termsFileChannel.force(true);
    }
}
