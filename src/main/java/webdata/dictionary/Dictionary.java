package webdata.dictionary;

import webdata.inverted_index.PostingListWriter;

import java.io.*;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Stream;

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
    private final TermsManager termsManager;

    // Contains the postings
    private static final String POSTINGS_FILE_NAME = "postings.bin";


    private static final Charset ENCODING = StandardCharsets.ISO_8859_1;
    private static final int BLOCK_SIZE = 4;


    private String curTerm;
    private int curTermPostingPtr;
    private int totalNumberOfTokens;
    private int uniqueNumberOfTokens;

    /**
     * Loads a dictionary(if directory exists), otherwise creates one.
     *
     * @param dir Directory of index files
     * @param encoding Encoding of terms
     * @param mmapSize Size of memory mapped file containing terms
     * @throws IOException In case of IO error
     */
    public Dictionary(String dir, Charset encoding, int mmapSize) throws IOException {
        this.dir = dir;

        if (dir.contains(TERMS_FILE_NAME) && dir.contains(POSTINGS_FILE_NAME)) {
            throw new RuntimeException("TODO Load dictionary");
        } else {
            Files.createDirectories(Paths.get(dir));

            var postingsFos = new FileOutputStream(Paths.get(dir, POSTINGS_FILE_NAME).toString());
            var postingsOs = new BufferedOutputStream(postingsFos);
            this.postingListWriter = new PostingListWriter(postingsOs);
            this.termsManager = new TermsManager(Paths.get(dir, TERMS_FILE_NAME).toString(), encoding, mmapSize);

            this.blocks = new ArrayList<>();
            this.curTerm = null;
            this.curTermPostingPtr = 0;
            this.totalNumberOfTokens = 0;
            this.uniqueNumberOfTokens = 0;

        }
    }

    /** Given a term pointer and length(in bytes), returns the term. */
    public CharBuffer derefTermPointer(int termPointer, int termLength) {
        return this.termsManager.derefTerm(termPointer, termLength);
    }

    /** Begins a new term */
    public void beginTerm(String term) throws IOException {
        if (postingListWriter.getCurrentTerm() != null && postingListWriter.getCurrentTerm().compareTo(term) >= 0)
        {
            throw new IllegalArgumentException("Terms must be written into dictionary in lexicographically increasing order");
        }
        // Finish the previous term(if there was any)
        endTerm();
        curTerm = term;
        curTermPostingPtr = postingListWriter.startTerm(term);
        uniqueNumberOfTokens++;
    }

    public void endTerm() throws IOException {
        if (curTerm == null) {
            return;
        }
        if (postingListWriter.getCurrentTermDocumentFrequency() == 0) {
            throw new IllegalStateException("You cannot end a term which has an empty posting list");
        }
        DictionaryBlock block;
        if (blocks.isEmpty() || blocks.get(blocks.size() - 1).full()) {
            block = new DictionaryBlock(this);
            blocks.add(block);
        } else {
            block = blocks.get(blocks.size() - 1);
        }

        var termAllocationResult = termsManager.allocateTerm(postingListWriter.getCurrentTerm());

        block.fillNewDictionaryElement(
                termAllocationResult.position,
                termAllocationResult.length,
                postingListWriter.getCurrentTermDocumentFrequency(),
                curTermPostingPtr
        );

        curTerm = null;
        curTermPostingPtr = 0;

    }

    public void addTerm(int docId, String[] reviewTerms) throws IOException {
        String term = postingListWriter.getCurrentTerm();
        long freqInDoc = Arrays.stream(reviewTerms)
                              .filter(someTerm -> someTerm.equals(term))
                              .count();
        addTermOccurence(docId, (int)freqInDoc);

    }

    public void addTermOccurence(int docId, int freqInDoc) throws IOException {
        totalNumberOfTokens += freqInDoc;
        postingListWriter.add(docId, freqInDoc);
    }

    /** Returns a stream over all dictionary elements */
    public Stream<DictionaryElement> stream() {
        return blocks.stream().flatMap(DictionaryBlock::stream);
    }

    /** Returns the number of tokens(including repetitions) seen in the dictionary */
    public int getTotalNumberOfTokens() {
        return totalNumberOfTokens;
    }

    /** Returns the number of different tokens in the dictionary.  */
    public int getUniqueNumberOfTokens() {
        return uniqueNumberOfTokens;
    }

    @Override
    public void close() throws IOException {
        postingListWriter.close();
        termsManager.close();;
    }

    @Override
    public void flush() throws IOException {
        postingListWriter.flush();
        termsManager.flush();
    }
}
