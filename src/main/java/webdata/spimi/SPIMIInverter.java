package webdata.spimi;

import webdata.inverted_index.PostingListWriter;

import java.io.*;
import java.util.*;


/** An in-memory index builder for creating temporary index files. */
public class SPIMIInverter {
    private final TreeMap<String, ArrayList<DocAndFreq>> dictionary;

    public SPIMIInverter() {
        dictionary = new TreeMap<>();
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

        while (hasMemory() && tokenStream.hasNext()) {
            Token token = tokenStream.next();
            var postingList = dictionary.computeIfAbsent(token.getTerm(),
                    _term -> new ArrayList<>());
            postingList.add(token.toDocAndFreq());

            assert lastToken == null || token.getDocID() >= lastToken.getDocID() : "tokenStream should be ordered by docIDs";
            lastToken = token;
        }

        serialize(os);
    }

    private void serialize(OutputStream os) throws IOException {
        try (var dos = new DataOutputStream(os);
             var writer = new PostingListWriter(dos)) {
            for (var entry:  dictionary.entrySet()) {
                var term = entry.getKey();
                var list = entry.getValue();

                dos.writeUTF(term);
                dos.writeInt(list.size());

                writer.startTerm(term);
                for (var docFreq: list) {
                    writer.add(docFreq.getDocID(), docFreq.getDocFrequency());
                }
                // flush the writer to ensure bytes are written to the DataOutputStream,
                // so they won't be mixed up with
                // Don't flush the stream itself as that might not be efficient
                writer.flushEncoderOnly();
            }
       }
    }

    private boolean hasMemory() {
        return true;
    }
}
