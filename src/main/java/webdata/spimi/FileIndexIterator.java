package webdata.spimi;

import webdata.DocAndFreq;
import webdata.compression.GroupVarintDecoder;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Allows iterating over a posting list within a temporary index file.
 * Can be reset in order to process multiple different postings within the same file.
 */
public class FileIndexIterator implements IndexIterator {
    private int lastDocId;
    private int documentFrequency;
    private String term;
    private int numDocsRead;
    private final DataInputStream dis;
    private final GroupVarintDecoder decoder;

    /**
     * Creates a posting iterator from an input stream oriented at the beginning of a posting list header
     *
     * @param is Input stream oriented at the beginning of a posting list's metadata
     * @throws IOException In case reading the posting list's metadata(term and document frequency) fails
     */
    public FileIndexIterator(InputStream is) throws IOException {
        this.dis = new DataInputStream(is);
        this.decoder = new GroupVarintDecoder(dis);
        moveToNextPostingList();
    }

    @Override
    public boolean moveToNextPostingList() throws IOException {
        if (dis.available() == 0) {
            return false;
        }
        this.lastDocId = 0;
        this.term = dis.readUTF();
        this.documentFrequency = dis.readInt();
        this.numDocsRead = 0;
        this.decoder.reset();
        return true;
    }

    @Override
    public boolean hasNext() {
        return this.numDocsRead < this.documentFrequency;
    }

    @Override
    public DocAndFreq next() {
        try {
            int gap = decoder.read();
            int freq = decoder.read();
            lastDocId = lastDocId + gap;
            ++numDocsRead;
            return new DocAndFreq(lastDocId, freq);
        } catch (IOException e) {
            throw new RuntimeException("IO exception while iterating posting list", e);
        }
    }

    @Override
    public int getDocumentFrequency() {
        return documentFrequency;
    }

    @Override
    public String getTerm() {
        return term;
    }
}
