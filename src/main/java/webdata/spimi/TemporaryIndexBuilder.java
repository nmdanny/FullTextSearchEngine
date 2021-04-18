package webdata.spimi;

import webdata.DocAndFreq;
import webdata.Token;
import webdata.Utils;
import webdata.dictionary.SequentialDictionaryBuilder;

import java.io.*;
import java.nio.file.Path;
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
     * @param indexPath Path of index
     * @throws IOException In case of IO failure when creating the index file
     */
    public void invert(Iterator<Token> tokenStream, Path indexPath) throws IOException {
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
        try (var builder = new SequentialDictionaryBuilder(indexPath.toString())) {
            serialize(builder);
            Utils.log("Finished creating temporary index at %s", indexPath);
        } finally {
            dictionary.clear();
            runtime.gc();
        }
        dictionary.clear();
        runtime.gc();
    }

    private void serialize(SequentialDictionaryBuilder builder) throws IOException {
        Utils.log("Beginning to sort and serialize temporary index, has %,d unique tokens, %,d total tokens",
                dictionary.size(), curNumberOfTokens);
        Utils.logMemory(runtime);
            var sortedEntries = dictionary.entrySet()
                    .stream()
                    .sorted(Map.Entry.comparingByKey())
                    .collect(Collectors.toList());
            for (var entry:  sortedEntries) {
                var term = entry.getKey();
                var list = entry.getValue();

                builder.beginTerm(term);
                for (var docFreq: list) {
                    builder.addTermOccurence(docFreq.getDocID(), docFreq.getDocFrequency());
                }
            }
    }

    private boolean hasMemory() {
        return Utils.getFreeMemory(runtime) >= MIN_MEMORY;
    }
}
