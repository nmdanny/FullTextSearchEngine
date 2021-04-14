package webdata.dictionary;

import webdata.Utils;
import webdata.compression.FrontCodingDecoder;
import webdata.compression.FrontCodingResult;
import webdata.inverted_index.PostingListReader;

import java.io.*;
import java.nio.charset.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/** Contains the in memory dictionary, allowing querying it */
public class Dictionary {

    private final String dir;

    private PackedDictionaryElements elements;
    static final int BLOCK_SIZE = 4;

    private final PostingListReader postingListReader;


    // Contains the terms
    static final String TERMS_FILE_NAME = "terms.txt";
    static final Charset TERMS_FILE_ENCODING = StandardCharsets.UTF_8;

    private final byte[] termsBuf;
    private ByteArrayInputStream termsIs;
    private final FrontCodingDecoder decoder;

    // Contains the dictionary elements
    static final String DICTIONARY_FILE_NAME = "dictionary.bin";

    static final String DICTIONARY_STATS_FILE = "dictionary-stats.bin";

    // Contains the postings
    static final String POSTINGS_FILE_NAME = "postings.bin";

    private int totalNumberOfTokens;
    private int uniqueNumberOfTokens;

    /**
     * @param dir Directory of index files
     * @throws IOException In case of IO error
     */
    public Dictionary(String dir) throws IOException {
        this.dir = dir;

//        if (Files.exists(Path.of(dir, DICTIONARY_FILE_NAME)) && Files.exists(Path.of(dir, POSTINGS_FILE_NAME))) {

        deserialize();
        this.postingListReader = new PostingListReader(Path.of(dir, POSTINGS_FILE_NAME).toString());

        this.termsBuf  = Files.readAllBytes(Paths.get(dir, TERMS_FILE_NAME));
        this.termsIs = new ByteArrayInputStream(this.termsBuf);

        this.decoder = new FrontCodingDecoder(
                Dictionary.BLOCK_SIZE,
                Dictionary.TERMS_FILE_ENCODING,
                this.termsIs
        );
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
    String getTerm(int index) {
        try {
            return getTermInner(index);
        } catch (IOException ex) {
            throw new RuntimeException("Impossible: IO exception for a ByteArrayInputStream", ex);
        }
    }

    private String getTermInner(int index) throws IOException {

        int posInBlock = index % BLOCK_SIZE;
        var firstBlockElement = (FirstBlockElement)this.elements.get(index - posInBlock);
        var frontCodingResult = new FrontCodingResult(
                firstBlockElement.suffixPos,
                0,
                firstBlockElement.suffixLength
        );

        this.termsIs = new ByteArrayInputStream(this.termsBuf);
        long skipped = this.termsIs.skip(frontCodingResult.suffixPos);
        assert skipped == frontCodingResult.suffixPos;
        decoder.reset(this.termsIs);

        String term = decoder.decodeElement(frontCodingResult);

        // now, iterate over the rest of the blocks if the position in the block isn't 0
        long curSuffixPos = frontCodingResult.suffixPos + frontCodingResult.suffixLengthBytes;
        for (int curIndex = index - posInBlock + 1; curIndex <= index; ++curIndex) {
            var otherBlockElement = (OtherBlockElement)this.elements.get(curIndex);
            frontCodingResult = new FrontCodingResult(
                    curSuffixPos,
                    otherBlockElement.prefixLength,
                    otherBlockElement.suffixLength
            );
            term = decoder.decodeElement(frontCodingResult);
            curSuffixPos += frontCodingResult.suffixLengthBytes;
        }

        return term;
    }

    /** Returns the number of documents containing at least 1 occurrences of the term at given index.
     *  Equivalently, this is the length of the posting list of said term. */
    public int getTokenFrequency(int index) {
        return this.elements.get(index).getTokenFrequency();
    }

    /** Returns a sequence of docID and freq pairs of documents containing term at given index. */
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
            return getTerm(index);
        }
    };

    /** Performs binary search over the dictionary in order to determine its index within the dictionary,
     * @param token Token
     * @return Index of token within dictionary, or -1 if it wasn't found
     */
    public int getIndexOfToken(String token) {
        return Collections.binarySearch(abstractList, token);
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

    /** Returns a stream of all different tokens in the dictionary, sorted lexicographically */
    public Stream<String> getTokens() {
        return IntStream.range(0, elements.size()).mapToObj(this::getTerm);
    }
}
