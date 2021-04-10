package webdata.inverted_index;

import webdata.compression.GroupVarintDecoder;

import java.io.*;
import java.nio.channels.Channels;
import java.util.Enumeration;
import java.util.NoSuchElementException;
import java.util.stream.IntStream;

public class PostingListReader {

    private final String filePath;

    public PostingListReader(String path) throws IOException {
        this.filePath = path;
    }

    /** Returns the posting list at given pointer as a stream of integers, without caring about what they convey,
     *  and without translating gaps to docIds. */
    public IntStream readIntegers(int postingPtr, int frequency) throws IOException {
        assert frequency > 0;

        var raf = new RandomAccessFile(filePath, "r");
        var fileChannel = raf.getChannel();
        fileChannel.position(postingPtr);
        var stream = new BufferedInputStream(Channels.newInputStream(fileChannel));
        final var decoder = new GroupVarintDecoder(stream);

        return IntStream.iterate(1, num -> num > 0, _prev -> {
            try {
                return decoder.read();

            } catch (IOException e) {
                System.err.format("Error while reading integers at ptr %d: %s", postingPtr, e);
                return -1;
            }
        }).skip(1).limit(frequency).onClose(() -> {
            try {
                stream.close();
                fileChannel.close();
                raf.close();
            } catch (IOException e) {
                System.err.format("IOException while closing resources: %s", e);
            }
        });
    }

    /** Returns the posting list as a stream of docIds and frequencies */
    public Enumeration<Integer> readDocIdFreqPairs(int postingPtr, int frequency) throws IOException {
        assert frequency > 0;

        var raf = new RandomAccessFile(filePath, "r");
        var fileChannel = raf.getChannel();
        fileChannel.position(postingPtr);
        var stream = new BufferedInputStream(Channels.newInputStream(fileChannel));
        final var decoder = new GroupVarintDecoder(stream);

        return new Enumeration<>() {

            // first element is docId
            private boolean isDocId = true;
            private int curDocId = 0;
            private int numPairsRead = 0;
            private int nextRead = decoder.read();

            private void finishReading() throws IOException {
                assert numPairsRead == frequency;
                decoder.close();
                fileChannel.close();
                raf.close();
            }

            @Override
            public boolean hasMoreElements() {
                return nextRead > 0;
            }

            @Override
            public Integer nextElement() {
                if (!hasMoreElements()) {
                    throw new NoSuchElementException();
                }
                int retVal;
                try {
                    if (isDocId) {
                        curDocId += nextRead;
                        retVal = curDocId;
                        isDocId = false;
                        nextRead = decoder.read();
                        assert (nextRead > 0) : "After decoding docId gap, expected to see a freq";
                    } else {
                        numPairsRead++;
                        retVal = nextRead;
                        isDocId = true;
                        nextRead = decoder.read();
                        if (nextRead == -1) {
                            finishReading();
                        }
                    }
                    return retVal;
                }
                catch (IOException ex) {
                    throw new NoSuchElementException(String.format("Encountered IO exception during processing of posting list iterator: %s", ex));
                }
            }
        };

    }
}
