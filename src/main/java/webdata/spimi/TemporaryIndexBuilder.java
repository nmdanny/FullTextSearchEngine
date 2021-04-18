package webdata.spimi;

import webdata.DocAndFreq;
import webdata.Token;
import webdata.Utils;
import webdata.inverted_index.PostingListWriter;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;


/** An in-memory index builder for creating temporary index files. */
public class TemporaryIndexBuilder {
    private final HashMap<String, ArrayList<DocAndFreq>> dictionary;
    private final Runtime runtime;

    // Ensure we have at least 10mb
    private static final long MIN_MEMORY = 1024 * 1024 * 10;

    // for logging
    private long curNumberOfTokens;
    private long totalNumberOfTokens;
    private static final long LOG_EVERY = 1000000;

    public TemporaryIndexBuilder() {
        dictionary = new HashMap<>();
        this.runtime = Runtime.getRuntime();
        this.curNumberOfTokens = 0;
        this.totalNumberOfTokens = 0;
    }

    /**
     * Performs 1 run of SPMI-Invert
     * @param tokenStream Token iterator (sorted by docIDs, naturally)
     * @param os Output stream of index file
     * @throws IOException In case of IO failure when creating the index file
     */
    public void invert(Iterator<Token> tokenStream, OutputStream os) throws IOException {
        dictionary.clear();
        Token lastToken = null;
        curNumberOfTokens = 0;

        while (hasMemory() && tokenStream.hasNext()) {
            Token token = tokenStream.next();
            var postingList = dictionary.computeIfAbsent(token.getTerm(),
                    _term -> new ArrayList<>());
            postingList.add(token.toDocAndFreq());

            assert lastToken == null || token.getDocID() >= lastToken.getDocID() : "tokenStream should be ordered by docIDs";
            lastToken = token;
            ++curNumberOfTokens;
            ++totalNumberOfTokens;
            if (totalNumberOfTokens % LOG_EVERY == 0) {
                Utils.log("Processed %,d tokens in the current index, a total of %,d tokens",
                          curNumberOfTokens, totalNumberOfTokens);
                Utils.logMemory(runtime);
            }
        }
        serialize(os);
        dictionary.clear();
        runtime.gc();
    }

    private void serialize(OutputStream os) throws IOException {
        Utils.log("Beginning to sort and serialize temporary index, has %,d unique tokens, %,d total tokens",
                dictionary.size(), curNumberOfTokens);
        Utils.logMemory(runtime);
        try (var dos = new DataOutputStream(os);
             var writer = new PostingListWriter(dos)) {
            var sortedEntries = dictionary.entrySet()
                    .stream()
                    .sorted(Map.Entry.comparingByKey())
                    .collect(Collectors.toList());
            for (var entry:  sortedEntries) {
                var term = entry.getKey();
                var list = entry.getValue();

                dos.writeUTF(term);
                dos.writeInt(list.size());

                writer.startTerm(term);
                for (var docFreq: list) {
                    writer.add(docFreq.getDocID(), docFreq.getDocFrequency());
                }
                // flush the writer to ensure posting list bytes are written to the DataOutputStream
                // and won't be mixed with ones from the data output stream next iteration
                writer.flushEncoderOnly();
            }
            Utils.log("Serialized a total of %,d bytes for temporary index\n", dos.size());
       }
    }

    private boolean hasMemory() {
        return Utils.getFreeMemory(runtime) >= MIN_MEMORY;
    }
}
