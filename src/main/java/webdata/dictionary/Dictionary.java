package webdata.dictionary;

import webdata.Token;
import webdata.Utils;
import webdata.compression.FrontCodingDecoder;
import webdata.compression.FrontCodingResult;
import webdata.compression.GroupVarintDecoder;
import webdata.inverted_index.PostingListReader;

import java.io.*;
import java.nio.charset.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Consumer;
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
    private int numberDocIdFreqPairs;

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
        numberDocIdFreqPairs = statsIs.readInt();
        assert uniqueNumberOfTokens <= totalNumberOfTokens;
        assert numberDocIdFreqPairs >= uniqueNumberOfTokens;
        assert numberDocIdFreqPairs <= totalNumberOfTokens;
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

    /** Returns a spliterator over all terms in the dictionary along with their document frequencies,
     *  lexicographically ordered. */
    public Spliterator<Map.Entry<String, Integer>> terms() {
        int sizeHint = uniqueNumberOfTokens;
        int characteristics = Spliterator.SIZED | Spliterator.NONNULL | Spliterator.ORDERED | Spliterator.DISTINCT;

        return new Spliterators.AbstractSpliterator<>(sizeHint, characteristics) {
            int dictIndex = 0;
            String curPrefix = null;
            int curSuffixPos = 0;
            final Spliterator<DictionaryElement> dictElements = elements.dictionaryElementsSpliterator();

            final FrontCodingDecoder decoder = new FrontCodingDecoder(
                    Dictionary.BLOCK_SIZE,
                    Dictionary.TERMS_FILE_ENCODING,
                    new ByteArrayInputStream(termsBuf)
            );

            @Override
            public boolean tryAdvance(Consumer<? super Map.Entry<String, Integer>> action) {
                return dictElements.tryAdvance(element -> {
                    try {
                        if (dictIndex % BLOCK_SIZE == 0) {
                            var firstBlockElement = (FirstBlockElement)element;
                            var frontCodingResult = new FrontCodingResult(
                                    firstBlockElement.suffixPos,
                                    0,
                                    firstBlockElement.suffixLength
                            );
                            curSuffixPos = firstBlockElement.suffixPos + firstBlockElement.suffixLength;
                            curPrefix = decoder.decodeElement(frontCodingResult);
                        } else {
                            var otherBlockElement = (OtherBlockElement)element;
                            var frontCodingResult = new FrontCodingResult(
                                    curSuffixPos,
                                    otherBlockElement.prefixLength,
                                    otherBlockElement.suffixLength
                            );
                            curSuffixPos += frontCodingResult.suffixLengthBytes;
                            curPrefix = decoder.decodeElement(frontCodingResult);
                        }
                        action.accept(new AbstractMap.SimpleEntry<>(curPrefix, element.getTokenFrequency()));
                        ++dictIndex;
                    } catch (IOException ex) {
                        throw new RuntimeException("Impossible, IO error dealing with byte arrays", ex);
                    }
                });
            }
        };
    }

    /** Returns a spliterator over all tokens in the dictionary, sorted by terms, followed by docIDs */
    public Spliterator<Token> tokens() throws IOException {
        int sizeHint = numberDocIdFreqPairs;
        int characteristics = Spliterator.SIZED | Spliterator.ORDERED | Spliterator.NONNULL | Spliterator.DISTINCT;

        return new Spliterators.AbstractSpliterator<Token>(sizeHint, characteristics) {


            final Spliterator<Map.Entry<String, Integer>> termSpliterator = terms();
            final InputStream postings = new BufferedInputStream(new FileInputStream(
                Path.of(dir, POSTINGS_FILE_NAME).toString()
            ));
            final GroupVarintDecoder decoder = new GroupVarintDecoder(postings);

            int curFrequency = 0;
            String currentTerm = null;
            int postingListIndex = 0;
            int lastDocID = 0;
            boolean closed = false;

            private void close() throws IOException {
                postings.close();
                decoder.close();
                closed = true;
            }

            @Override
            public boolean tryAdvance(Consumer<? super Token> action) {
                try {
                    if (postingListIndex >= curFrequency) {
                        // advance to next term
                        var hasMoreTerms = termSpliterator.tryAdvance(newTermFreq -> {
                            currentTerm = newTermFreq.getKey();
                            curFrequency = newTermFreq.getValue();
                            postingListIndex = 0;
                            assert curFrequency >= 1;
                            try {
                                // there might be multiple zeros
                                do {
                                    lastDocID = decoder.read();
                                } while (lastDocID == 0);
                                var freqInDoc = decoder.read();
                                action.accept(new Token(currentTerm, lastDocID, freqInDoc));
                                ++postingListIndex;
                            } catch (IOException e) {
                                throw new RuntimeException("Got IO exception while dealing with posting list", e);
                            }
                        });
                        if (!hasMoreTerms && !closed) {
                            close();
                        }
                        return hasMoreTerms;
                    } else {
                        // advance to next posting list entry
                        int gap = decoder.read();
                        assert gap > 0;
                        lastDocID += gap;
                        var freqInDoc = decoder.read();
                        action.accept(new Token(currentTerm, lastDocID, freqInDoc));
                        ++postingListIndex;
                        return true;
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Got IO exception while dealing with posting list", e);
                }
            }
        };
    }
}
