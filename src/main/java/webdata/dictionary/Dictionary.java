package webdata.dictionary;

import webdata.inverted_index.PostingListReader;
import webdata.inverted_index.PostingListWriter;

import java.io.*;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Stream;

/** Contains the in memory dictionary, allowing reading and writing to it, as well as saving and loading to disk */
public class Dictionary implements Closeable, Flushable {

    private final String dir;

    private final ArrayList<DictionaryElement> elements;
    private static final int BLOCK_SIZE = 4;

    private final PostingListWriter postingListWriter;
    private final PostingListReader postingListReader;


    // Contains the terms
    private static final String TERMS_FILE_NAME = "terms.bin";
    private final TermsManager termsManager;

    // Contains the dictionary elements
    private static final String DICTIONARY_FILE_NAME = "dictionary.bin";

    // Contains the postings
    private static final String POSTINGS_FILE_NAME = "postings.bin";

    private String curTerm;
    private int curTermPostingPtr;
    private int totalNumberOfTokens;
    private int uniqueNumberOfTokens;

    /**
     * @param dir Directory of index files
     * @param encoding Encoding of terms
     * @param mmapSize Size of memory mapped file containing terms
     * @throws IOException In case of IO error
     */
    public Dictionary(String dir, Charset encoding, int mmapSize) throws IOException {
        this.dir = dir;

        this.curTerm = null;
        this.curTermPostingPtr = -1;

        if (Files.exists(Path.of(dir, DICTIONARY_FILE_NAME)) && Files.exists(Path.of(dir, POSTINGS_FILE_NAME))) {
            this.elements = new ArrayList<>();
            deserialize();
        } else {
            Files.createDirectories(Paths.get(dir));
            this.elements = new ArrayList<>();
            this.totalNumberOfTokens = 0;
            this.uniqueNumberOfTokens = 0;
        }

        var postingsFos = new FileOutputStream(Paths.get(dir, POSTINGS_FILE_NAME).toString());
        var postingsOs = new BufferedOutputStream(postingsFos);
        this.postingListWriter = new PostingListWriter(postingsOs);
        this.postingListReader = new PostingListReader(Path.of(dir, POSTINGS_FILE_NAME).toString());
        this.termsManager = new TermsManager(Paths.get(dir, TERMS_FILE_NAME).toString(), encoding, mmapSize);
    }

    public void serialize(DataOutputStream os) throws IOException {
        // write constant statistics
        os.writeInt(totalNumberOfTokens);
        os.writeInt(uniqueNumberOfTokens);

        assert uniqueNumberOfTokens == elements.size();
        // write elements
        for (int i=0; i < uniqueNumberOfTokens; ++i) {
            if (i % BLOCK_SIZE == 0) {
                ((FirstBlockElement)elements.get(i)).serialize(os);
            } else {
                ((OtherBlockElement)elements.get(i)).serialize(os);
            }
        }
    }

    public void serialize() throws IOException {
        var os = new DataOutputStream(
                new BufferedOutputStream(
                        new FileOutputStream(Path.of(dir, DICTIONARY_FILE_NAME).toString())
                )
        );
        serialize(os);
        os.flush();
        os.close();
    }

    public void deserialize(DataInputStream is) throws IOException {
        if (elements.size() != 0) {
            throw new IllegalStateException("Cannot deserialize into non empty dictionary");
        }

        // read constant statistics
        totalNumberOfTokens = is.readInt();
        uniqueNumberOfTokens = is.readInt();

        // read elements
        elements.ensureCapacity(uniqueNumberOfTokens);
        for (int i=0; i < uniqueNumberOfTokens; ++i) {
            if (i % BLOCK_SIZE == 0) {
                elements.add(FirstBlockElement.deserialize(is));
            } else {
                elements.add(OtherBlockElement.deserialize(is));
            }
        }


    }

    public void deserialize() throws IOException {
        var is = new DataInputStream(
                new BufferedInputStream(
                        new FileInputStream(Path.of(dir, DICTIONARY_FILE_NAME).toString())
                )
        );
        deserialize(is);
        is.close();
    }

    /** Given a term pointer and length(in bytes), returns the term. */
    public CharBuffer derefTermPointer(int termPointer, int termLength) {
        return this.termsManager.derefTerm(termPointer, termLength);
    }

    /** Returns the term of this element, assuming its index is known */
    CharBuffer getTerm(int index) {
        if (index % BLOCK_SIZE == 0) {
            var element = ((FirstBlockElement)this.elements.get(index));
            return derefTermPointer(element.termPointer, element.termLength);
        } else {
            int firstBlockIndex = index - index % BLOCK_SIZE;
            var firstBlockElement = ((FirstBlockElement)this.elements.get(firstBlockIndex));

            int termPointer = firstBlockElement.termPointer + firstBlockElement.termLength;
            for (int prevBlockIndex = firstBlockIndex + 1; prevBlockIndex < index; ++prevBlockIndex) {
                termPointer += ((OtherBlockElement)this.elements.get(prevBlockIndex)).termLength;
            }

            var selectedBlockElement = ((OtherBlockElement)this.elements.get(index));
            return derefTermPointer(termPointer, selectedBlockElement.termLength);
        }
    }

    /** Returns the number of documents containing at least 1 occurrences of the term at given index.
     *  Equivalently, this is the length of the posting list of said term. */
    public int getTokenFrequency(int index) {
        return this.elements.get(index).getTokenFrequency();
    }

    /** Gets a pointer to the postings list of term at given index */
    int getPostingsPointer(int index) {
        return this.elements.get(index).getPostingsPointer();
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

        var termAllocationResult = termsManager.allocateTerm(postingListWriter.getCurrentTerm());

        // begin a new block
        if (elements.size() % BLOCK_SIZE == 0) {
            var element = new FirstBlockElement(
                    postingListWriter.getCurrentTermDocumentFrequency(),
                    curTermPostingPtr,
                    termAllocationResult.length,
                    termAllocationResult.position);
            elements.add(element);
        } else {
            // add to previous block
            var element = new OtherBlockElement(
                    postingListWriter.getCurrentTermDocumentFrequency(),
                    curTermPostingPtr,
                    termAllocationResult.length
            );
            elements.add(element);

        }

        curTerm = null;
        curTermPostingPtr = -1;

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
        return elements.stream();
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
        flush();
        postingListWriter.close();
        termsManager.close();
    }

    @Override
    public void flush() throws IOException {
        postingListWriter.flush();
        termsManager.flush();
        serialize();
    }
}
