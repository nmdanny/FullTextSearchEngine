package webdata.inverted_index;

import webdata.compression.GroupVarintDecoder;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.util.stream.IntStream;

public class PostingListReader implements Closeable {

    private final RandomAccessFile postingsFile;
    private final FileChannel fileChannel;

    private int lastDocId;
    private String curTerm;
    private int curDocumentFrequency;
    private int curPostingPtr;

    public PostingListReader(RandomAccessFile postingsFile) throws IOException {
        this.postingsFile = postingsFile;
        this.fileChannel = postingsFile.getChannel();

        this.lastDocId = 0;
        this.curTerm = null;
        this.curDocumentFrequency = 0;
        this.curPostingPtr = 0;
    }

    /** Returns the posting list at given pointer as a stream of integers(without caring about what they convey) */
    public IntStream readIntegers(int postingPtr, int frequency) throws IOException {
        postingsFile.seek(postingPtr);
        var stream = Channels.newInputStream(fileChannel);
        final var decoder = new GroupVarintDecoder(stream);
        return IntStream.iterate(1, num -> num > 0, _prev -> {
            try {
                return decoder.read();

            } catch (IOException e) {
                System.err.format("Error while reading integers at ptr %d: %s", postingPtr, e);
                return -1;
            }
        }).skip(1).limit(frequency);
    }

    @Override
    public void close() throws IOException {
        fileChannel.close();
        postingsFile.close();
    }
}
