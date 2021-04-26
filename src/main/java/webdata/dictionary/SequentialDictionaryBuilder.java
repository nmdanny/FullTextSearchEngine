package webdata.dictionary;

import webdata.Token;
import webdata.compression.FrontCodingEncoder;
import webdata.inverted_index.PostingListWriter;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;

/** Allows building a dictionary into disk in a sequential manner, by adding terms and their occurrences (sorted by
 *  terms and docIds), though they don't have to be in memory. */
public class SequentialDictionaryBuilder implements Closeable, Flushable, DictionaryBuilder {

    private final String dir;
    private final DataOutputStream elementsDos;

    private final PostingListWriter postingListWriter;
    private final FrontCodingEncoder encoder;

    private String curTerm;
    private long curTermPostingPtr;
    private int lastDocId;
    private int lastDocFreq;
    private long totalNumberOfTokens;
    private int uniqueNumberOfTokens;
    private long numberDocIdFreqPairs;

    private FirstBlockElement lastFbe;

    public SequentialDictionaryBuilder(String dir) throws IOException {
        this.dir = dir;
        this.curTerm = null;
        this.curTermPostingPtr = -1;
        this.lastDocId = 0;
        this.lastDocFreq = 0;

        Files.createDirectories(Paths.get(dir));
        this.totalNumberOfTokens = 0;
        this.uniqueNumberOfTokens = 0;
        this.numberDocIdFreqPairs = 0;


        var elementsFos = new FileOutputStream(Paths.get(dir, Dictionary.DICTIONARY_FILE_NAME).toString(), false);
        this.elementsDos = new DataOutputStream(new BufferedOutputStream(elementsFos));

        var postingsFos = new FileOutputStream(Paths.get(dir, Dictionary.POSTINGS_FILE_NAME).toString(), false);
        var postingsOs = new BufferedOutputStream(postingsFos);
        this.postingListWriter = new PostingListWriter(postingsOs);

        this.encoder = new FrontCodingEncoder(
                Dictionary.BLOCK_SIZE,
                Dictionary.TERMS_FILE_ENCODING,
                new BufferedOutputStream(new FileOutputStream(Paths.get(dir, Dictionary.TERMS_FILE_NAME).toString(), false))
        );
    }

    /** Adds given token to dictionary. Tokens must arrive lexicographically ordered by
     *  term, followed by docID. Duplicate tokens(same term and docID, e.g, from different dictionaries)
     *  are allowed. */
    public void addToken(Token token) throws IOException {
        if (!token.getTerm().equals(curTerm)) {
            beginTerm(token.getTerm());
        }
        addTermOccurence(token.getDocID(), token.getDocFrequency());
    }

    /** Begins a new term */
    @Override
    public void beginTerm(String term) throws IOException {
        assert !term.isEmpty() : "Terms should not be empty";
        if (postingListWriter.getCurrentTerm() != null && postingListWriter.getCurrentTerm().compareTo(term) > 0)
        {
            throw new IllegalArgumentException("Terms must be written into dictionary in lexicographically increasing order");
        }
        // Finish the previous term(if there was any)
        endTerm();
        curTerm = term;
        curTermPostingPtr = postingListWriter.startTerm(term);
    }

    @Override
    public void endTerm() throws IOException {
        if (curTerm == null) {
            return;
        }
        endTermOccurence();
        if (postingListWriter.getCurrentTermDocumentFrequency() == 0) {
            throw new IllegalStateException("You cannot end a term which has an empty posting list");
        }

        var frontCodingResult = encoder.encodeString(postingListWriter.getCurrentTerm());

        assert (frontCodingResult.suffixPos >= 0) : "Can only support terms file with 2^31 chars";

        // begin a new block
        if (uniqueNumberOfTokens % Dictionary.BLOCK_SIZE == 0) {
            assert frontCodingResult.prefixLength == 0;
            var element = new FirstBlockElement(
                    postingListWriter.getCurrentTermDocumentFrequency(),
                    curTermPostingPtr,
                    frontCodingResult.suffixLength,
                    frontCodingResult.suffixPos
            );
            element.serialize(elementsDos);
            this.lastFbe = element;
        } else {
            // add to previous block
            long gap = curTermPostingPtr - this.lastFbe.postingPtr;
            assert ((int) gap == gap) : "Posting pointer gap within block can be at most 2^31";
            var element = new OtherBlockElement(
                    this.lastFbe,
                    postingListWriter.getCurrentTermDocumentFrequency(),
                    (int)gap,
                    frontCodingResult.prefixLength,
                    frontCodingResult.suffixLength
            );
            element.serialize(elementsDos);
        }

        curTerm = null;
        curTermPostingPtr = -1;
        uniqueNumberOfTokens++;

    }

    @Override
    public void addTermOccurence(int docId, int freqInDoc) throws IOException {
        totalNumberOfTokens += freqInDoc;
        assert docId >= lastDocId;
        if (docId > lastDocId) {
            endTermOccurence();
            lastDocId = docId;
            lastDocFreq = freqInDoc;
        } else {
            lastDocFreq += freqInDoc;
        }
    }

    private void endTermOccurence() throws IOException {
        if (lastDocId == 0) {
            return;
        }
        postingListWriter.add(lastDocId, lastDocFreq);
        ++numberDocIdFreqPairs;
        lastDocId = 0;
        lastDocFreq = 0;
    }

    @Override
    public void close() throws IOException {
        endTerm();
        flush();
        postingListWriter.close();
        encoder.close();
        elementsDos.close();
    }

    @Override
    public void flush() throws IOException {
        postingListWriter.flush();
        encoder.flush();
        elementsDos.flush();

        var statFile = Paths.get(dir, Dictionary.DICTIONARY_STATS_FILE).toFile();
        try (var statsFos = new BufferedOutputStream(new FileOutputStream(statFile, false));
             var statsOs = new DataOutputStream(statsFos)) {
            statsOs.writeLong(totalNumberOfTokens);
            statsOs.writeInt(uniqueNumberOfTokens);
            statsOs.writeLong(numberDocIdFreqPairs);
            statsOs.flush();
        }
    }
}
