package webdata.spimi;

import webdata.DocAndFreq;
import webdata.Token;
import webdata.Utils;
import webdata.compression.Varint;
import webdata.dictionary.SequentialDictionaryBuilder;

import java.io.*;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

class PostingByteStream extends ByteArrayOutputStream {
    private int lastDocId = 0;
    private final static int DEFAULT_BYTE_CAPACITY = 2;
    PostingByteStream() {
        super(DEFAULT_BYTE_CAPACITY);
    }


    void add(Token token) {
        int docId = token.getDocID();
        int freq = token.getDocFrequency();
        int gap = docId - lastDocId;
        this.lastDocId = docId;
        try {
            Varint.encode(this, gap);
            Varint.encode(this, freq);
        } catch (IOException ex) {
            throw new RuntimeException("Impossible(IO error in byte array)", ex);
        }
    }

    Spliterator<DocAndFreq> tokens() {
        long sizeHint = Long.MAX_VALUE;
        int cs = Spliterator.ORDERED;

        var is = new ByteArrayInputStream(buf);
        return new Spliterators.AbstractSpliterator<DocAndFreq>(sizeHint, cs) {
            int lastDocId = 0;
            @Override
            public boolean tryAdvance(Consumer<? super DocAndFreq> action) {
                if (is.available() == 0) {
                    return false;
                }
                try {
                    int gap = Varint.decode(is);
                    if (gap <= 0) {
                        return false;
                    }
                    int freq = Varint.decode(is);
                    lastDocId += gap;
                    action.accept(new DocAndFreq(lastDocId, freq));
                    return true;
                } catch (IOException ex) {
                    throw new RuntimeException("Impossible(IO error in byte array)", ex);
                }
            }
        };
    }
}

/** An in-memory index builder for creating temporary index files. */
public class TemporaryIndexBuilder {
    private final HashMap<String, PostingByteStream> dictionary;
    private final Runtime runtime;

    // Ensure we have at least 10mb
    private static final long MIN_MEMORY = 1024 * 1024 * 10;

    // for logging
    public long curNumberOfTokens;
    public long totalNumberOfTokens;
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
                    _term -> new PostingByteStream());
            postingList.add(token);

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
                list.tokens().forEachRemaining(docAndFreq -> {
                    try {
                        builder.addTermOccurence(docAndFreq.getDocID(), docAndFreq.getDocFrequency());
                    } catch (IOException e) {
                        throw new RuntimeException("IO exception while adding term occurrence", e);
                    }
                });
            }
    }

    private boolean hasMemory() {
        return Utils.getFreeMemory(runtime) >= MIN_MEMORY;
    }
}
