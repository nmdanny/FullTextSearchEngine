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

/** Contains the in memory dictionary, allowing querying it */
public class Dictionary {

    private final String dir;
    private final Charset encoding;
    private final CharsetDecoder charsetDecoder;

    private PackedDictionaryElements elements;
    static final int BLOCK_SIZE = 4;

    private final PostingListReader postingListReader;


    // Contains the terms
    static final String TERMS_FILE_NAME = "terms.bin";
    private final TermsManager termsManager;

    // Contains the dictionary elements
    static final String DICTIONARY_FILE_NAME = "dictionary.bin";

    static final String DICTIONARY_STATS_FILE = "dictionary-stats.bin";

    // Contains the postings
    static final String POSTINGS_FILE_NAME = "postings.bin";

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


//        if (Files.exists(Path.of(dir, DICTIONARY_FILE_NAME)) && Files.exists(Path.of(dir, POSTINGS_FILE_NAME))) {

        deserialize();
        this.postingListReader = new PostingListReader(Path.of(dir, POSTINGS_FILE_NAME).toString());
        this.termsManager = new TermsManager(Paths.get(dir, TERMS_FILE_NAME).toString(), encoding, mmapSize);
    }

    public void deserialize(DataInputStream statsIs, DataInputStream elementsIs) throws IOException {
        // read constant statistics
        totalNumberOfTokens = statsIs.readInt();
        uniqueNumberOfTokens = statsIs.readInt();
        assert uniqueNumberOfTokens <= totalNumberOfTokens;
        elements = new PackedDictionaryElements(elementsIs, uniqueNumberOfTokens);
    }

    public void deserialize() throws IOException {
        var statsIs = new DataInputStream(
                new BufferedInputStream(
                        new FileInputStream(Path.of(dir, DICTIONARY_STATS_FILE).toString())
                )
        );

        var elementsIs = new DataInputStream(
                new BufferedInputStream(
                        new FileInputStream(Path.of(dir, DICTIONARY_FILE_NAME).toString())
                )
        );

        deserialize(statsIs, elementsIs);
        statsIs.close();
        elementsIs.close();
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
}
