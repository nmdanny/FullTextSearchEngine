package webdata;

import webdata.compression.GroupVarintEncoder;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.Objects;

/** Used for writing posting lists */
public class PostingListWriter implements Closeable, Flushable {

    // only used to determine position within file
    private final FileChannel fileChannel;
    private final GroupVarintEncoder encoder;

    private int lastDocId;
    private String curTerm;
    private int curDocumentFrequency;

    public PostingListWriter(FileOutputStream fileOutputStream, BufferedOutputStream outputStream) {
        this.fileChannel = fileOutputStream.getChannel();
        this.encoder = new GroupVarintEncoder(outputStream);

        this.lastDocId = 0;
        this.curTerm = null;
        this.curDocumentFrequency = 0;
    }

    /**
     * Adds an entry to the posting list of the current term
     * @param docId ID of document in which term of posting list appears
     * @param freq Number
     */
    public void add(int docId, int freq) throws IOException
    {
        if (Objects.isNull(curTerm)) {
            throw new IllegalStateException("Cannot add a posting list entry if no term was set");
        }
        if (docId <= lastDocId) {
            throw new IllegalArgumentException("docIds must be inserted in an increasing order");
        }
        if (freq <= 0) {
            throw new IllegalArgumentException("Frequency within document must be positive");
        }
        int gap = docId - lastDocId;
        encoder.write(gap);
        encoder.write(freq);
        curDocumentFrequency++;
        lastDocId = docId;
    }

    /** Resets the writer for writing a new posting list(or the first one), and returns a
     *  pointer to said posting list.*/
    public int startTerm(String term) throws IOException
    {
        // If we already wrote a posting list, reset the encoder to ensure a 0 is written
        if (lastDocId != 0) {
            encoder.reset();
        }
        lastDocId = 0;
        curTerm = term;
        curDocumentFrequency = 0;
        long pos = fileChannel.position();
        assert (int)pos == pos;
        return (int)pos;
    }

    /** Returns the number of documents in which the current term was seen */
    public int getCurrentTermDocumentFrequency() {
        return curDocumentFrequency;
    }

    /** Returns the current term whose posting lists we're building */
    public String getCurrentTerm() {
       return curTerm;
    }

    @Override
    public void close() throws IOException {
        this.fileChannel.close();
        this.encoder.close();
    }

    @Override
    public void flush() throws IOException {
        this.encoder.flush();
    }
}
