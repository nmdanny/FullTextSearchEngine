package webdata.inverted_index;

import webdata.compression.GroupVarintDecoder;

import java.io.*;
import java.nio.channels.Channels;
import java.util.Enumeration;
import java.util.NoSuchElementException;

/** Allows random access reads of an inverted index file, reading
 *  a posting list at any valid posting pointer.
 * */
public class PostingListReader {

    private final String filePath;

    public PostingListReader(String path) throws IOException {
        this.filePath = path;
    }

    /** Returns the posting list as a stream of docIds and frequencies */
    public Enumeration<Integer> readDocIdFreqPairs(int postingPtr, int frequency) throws IOException {
        assert frequency > 0;

        // TODO: maybe re-use file, need to check if enumeration will be used concurrently
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
                return nextRead > 0 && numPairsRead < frequency;
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
