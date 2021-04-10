package webdata.dictionary;

import webdata.Utils;
import webdata.inverted_index.PostingListReader;
import webdata.inverted_index.PostingListWriter;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

/** Contains the in memory dictionary, allowing reading and writing to it, as well as saving and loading to disk */
public class Dictionary implements Closeable, Flushable {

    private final String dir;
    private final Charset encoding;
    private final CharsetDecoder charsetDecoder;

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
        this.encoding = encoding;
        this.charsetDecoder = encoding.newDecoder()
                .onMalformedInput(CodingErrorAction.REPLACE)
                .onUnmappableCharacter(CodingErrorAction.REPLACE);

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

    /** Returns the term of this element, assuming its index is known */
    ByteBuffer getTerm(int index) {
        if (index % BLOCK_SIZE == 0) {
            var element = ((FirstBlockElement)this.elements.get(index));
            return termsManager.derefTermBytes(element.termPointer, element.termLength);
        } else {
            int firstBlockIndex = index - index % BLOCK_SIZE;
            var firstBlockElement = ((FirstBlockElement)this.elements.get(firstBlockIndex));

            int termPointer = firstBlockElement.termPointer + firstBlockElement.termLength;
            for (int prevBlockIndex = firstBlockIndex + 1; prevBlockIndex < index; ++prevBlockIndex) {
                termPointer += ((OtherBlockElement)this.elements.get(prevBlockIndex)).termLength;
            }

            var selectedBlockElement = ((OtherBlockElement)this.elements.get(index));
            return termsManager.derefTermBytes(termPointer, selectedBlockElement.termLength);
        }
    }

    /** Returns the number of documents containing at least 1 occurrences of the term at given index.
     *  Equivalently, this is the length of the posting list of said term. */
    public int getTokenFrequency(int index) {
        return this.elements.get(index).getTokenFrequency();
    }

    /** Returns a sequence of docID & freq pairs of documents containing term at given index. */
    public Enumeration<Integer> getDocIdsAndFreqs(int index) throws IOException {
        var element = this.elements.get(index);
        try {
            return postingListReader.readDocIdFreqPairs(element.getPostingsPointer(), element.getTokenFrequency());
        } catch (IOException e) {
            System.err.println("Couldn't get docIDs of postings for review at index " + index + ": " + e);
            return Utils.streamToEnumeration(Stream.empty());
        }
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

    public void addTermOccurence(int docId, int freqInDoc) throws IOException {
        totalNumberOfTokens += freqInDoc;
        postingListWriter.add(docId, freqInDoc);
    }


    /** Allows treating the dictionary as a list of byte buffers(term encodings),
     *  allowing to perform binary search over them
     */
    private final AbstractList<String> abstractList = new AbstractList<>() {
        @Override
        public int size() {
            return elements.size();
        }

        @Override
        public String get(int index) {
            try {
                return charsetDecoder.decode(getTerm(index)).toString();
            } catch (CharacterCodingException e) {
               throw new RuntimeException("Impossible: charset decoding failed");
            }
        }
    };

    /** Performs binary search over the dictionary in order to determine its index within the dictionary,
     * @param token Token
     * @return Index of token within dictionary, or -1 if it wasn't found
     */
    public int getIndexOfToken(String token) {
        return Collections.binarySearch(abstractList, token);
    }

    /** Returns the encoding used by this dictionary. */
    public Charset getEncoding() {
        return encoding;
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
