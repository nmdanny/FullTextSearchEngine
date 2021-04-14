package webdata.dictionary;

import webdata.inverted_index.PostingListWriter;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

/** Allows building a dictionary into disk in a sequential manner, by adding terms and their occurrences (sorted by
 *  terms and docIds), though they don't have to be in memory. */
public class SequentialDictionaryBuilder implements Closeable, Flushable {

    private final String dir;
    private final DataOutputStream elementsDos;

    private final PostingListWriter postingListWriter;
    private final TermsManager termsManager;

    private String curTerm;
    private int curTermPostingPtr;
    private int totalNumberOfTokens;
    private int uniqueNumberOfTokens;

    public SequentialDictionaryBuilder(String dir, Charset encoding, int mmapSize) throws IOException {
        this.dir = dir;
        this.curTerm = null;
        this.curTermPostingPtr = -1;

        Files.createDirectories(Paths.get(dir));
        this.totalNumberOfTokens = 0;
        this.uniqueNumberOfTokens = 0;


        var elementsFos = new FileOutputStream(Paths.get(dir, Dictionary.DICTIONARY_FILE_NAME).toString(), false);
        this.elementsDos = new DataOutputStream(new BufferedOutputStream(elementsFos));

        var postingsFos = new FileOutputStream(Paths.get(dir, Dictionary.POSTINGS_FILE_NAME).toString(), false);
        var postingsOs = new BufferedOutputStream(postingsFos);
        this.postingListWriter = new PostingListWriter(postingsOs);
        this.termsManager = new TermsManager(Paths.get(dir, Dictionary.TERMS_FILE_NAME).toString(), encoding, mmapSize);
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
        if (uniqueNumberOfTokens % Dictionary.BLOCK_SIZE == 0) {
            var element = new FirstBlockElement(
                    postingListWriter.getCurrentTermDocumentFrequency(),
                    curTermPostingPtr,
                    termAllocationResult.length,
                    termAllocationResult.position);
            element.serialize(elementsDos);
        } else {
            // add to previous block
            var element = new OtherBlockElement(
                    postingListWriter.getCurrentTermDocumentFrequency(),
                    curTermPostingPtr,
                    termAllocationResult.length
            );
            element.serialize(elementsDos);
        }

        curTerm = null;
        curTermPostingPtr = -1;
        uniqueNumberOfTokens++;

    }

    public void addTermOccurence(int docId, int freqInDoc) throws IOException {
        totalNumberOfTokens += freqInDoc;
        postingListWriter.add(docId, freqInDoc);
    }

    @Override
    public void close() throws IOException {
        flush();
        postingListWriter.close();
        termsManager.close();
        elementsDos.close();
    }

    @Override
    public void flush() throws IOException {
        postingListWriter.flush();
        termsManager.flush();
        elementsDos.flush();

        var statFile = Paths.get(dir, Dictionary.DICTIONARY_STATS_FILE).toFile();
        try (var statsFos = new BufferedOutputStream(new FileOutputStream(statFile, false));
             var statsOs = new DataOutputStream(statsFos)) {
            statsOs.writeInt(totalNumberOfTokens);
            statsOs.writeInt(uniqueNumberOfTokens);
            statsOs.flush();
        }
    }
}