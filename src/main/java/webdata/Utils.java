package webdata;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Utils {

    public static <T> Stream<T> iteratorToStream(Iterator<T> iterator)
    {
       return StreamSupport.stream(
               Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED),
               false
       );
    }

    /** Similar to `String::lastIndexOf` */
    public static int lastIndexOf(ByteBuffer sequence, ByteBuffer pattern) {
        if (sequence.remaining() < pattern.remaining()) {
            return -1;
        }
        outerloop:
        for (int start = sequence.remaining() - pattern.remaining(); start >= 0; --start) {
            for (int offset = 0; offset < pattern.remaining(); ++offset) {
                if (sequence.get(start + offset) != pattern.get(offset)) {
                    continue outerloop;
                }
            }
            return start;
        }
        return -1;
    }


    /***
     * Splits the given file containing reviews into a stream of memory mapped buffers, splitting by the given delimiter,
     * such that every sequence begins with said delimiter(namely, includes it)
     * @param fileName Name of file encoded.
     * @param bufSize Expected size of a buffer. Note that actual buffers might be slightly smaller, or much
     *                larger in case this buffer isn't enough to accommodate one review.
     * @param delimiter A delimiter
     * @return A stream of buffers, not thread-safe and shouldn't be run in parallel
     * @throws IOException In case of IO errors
     */
    public static Stream<MappedByteBuffer> splitFile(String fileName, int bufSize, ByteBuffer delimiter) throws IOException {
        var file = new RandomAccessFile(fileName, "r");
        final var channel = file.getChannel();
        long fileSize = file.length();

        // Yielded during every iteration of the stream
        // contains the buffer to be emitted by the final stream,
        // and state needed for the next iteration
        class StreamState {
            long bufPos;
            MappedByteBuffer toEmit;

            StreamState() {
                bufPos = 0;
                toEmit = null;
            }
        }

        return Stream.iterate(new StreamState(), lastStreamState -> {

            StreamState newStreamState = new StreamState();
            try {
                if (lastStreamState.bufPos >= fileSize) {
                    // finish the stream
                    newStreamState.bufPos = fileSize;
                    return newStreamState;
                }

                long size = Long.min(bufSize, fileSize - lastStreamState.bufPos);
                MappedByteBuffer mmBuf = channel.map(FileChannel.MapMode.READ_ONLY, lastStreamState.bufPos, size);

                int lastReviewStart = lastIndexOf(mmBuf, delimiter);

                if (lastReviewStart == -1)
                {
                    System.err.printf("Found a block with no reviews in indices %d-%d\n",
                            lastStreamState.bufPos, lastStreamState.bufPos + size);
                    newStreamState.bufPos = lastStreamState.bufPos + mmBuf.remaining();
                    return newStreamState;
                }

                // If we only detected the beginning of one review, and we haven't reached the end of the file
                // then it's likely the review takes up more than `bufSize` and got cut off in the end
                // of the buffer, which implies the bufSize is not enough
                if (lastReviewStart == 0 &&  size != fileSize - lastStreamState.bufPos)
                {
                    throw new IllegalArgumentException(
                            "The buffer size " + size + " isn't sufficient to hold a single review"
                    );
                }

                // if we have more than 1 review, and we haven't reached the end of the file,
                // since it's likely the last review(whose beginning does appear in our buffer)
                // got cut off, so handle it in the next buffer
                // by shrinking the current buffer to exclude said review
                if (lastReviewStart > 0 && size != fileSize - lastStreamState.bufPos) {
                    assert lastReviewStart < size;
                    mmBuf.limit(lastReviewStart);
                }


                newStreamState.bufPos = lastStreamState.bufPos + mmBuf.remaining();
                newStreamState.toEmit = mmBuf;
                return newStreamState;
            } catch (IOException ex) {
                System.err.println("IO exception while generating mapped byte buffer beginning at " + lastStreamState.bufPos + ": " + ex);
                return newStreamState;
            }
        }).skip(1).takeWhile(state -> state.toEmit != null).map(state -> state.toEmit);

    }

}