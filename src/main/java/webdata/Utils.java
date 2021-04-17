package webdata;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Utils {

    public static void log(String format, Object... args)
    {
        System.out.format(
                "[" + new SimpleDateFormat("HH:mm.ss").format(new Date()) + "] " +
                        format + "\n", args);
    }

    public static <T> int binarySearchLeftmost(List<? extends Comparable<? super T>> list, T target) {
        int low = 0;
        int high = list.size();
        while (low < high) {
            int mid = (low + high)/2;
            Comparable<? super T> midCandidate = list.get(mid);
            if (midCandidate.compareTo(target) < 0) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        if (low < list.size() && list.get(low).compareTo(target) == 0) {
            return low;
        }
        return -1;
    }

    public static <T> int binarySearchRightmost(List<? extends Comparable<? super T>> list, T target) {
        int low = 0;
        int high = list.size();
        while (low < high) {
            int mid = (low + high)/2;
            Comparable<? super T> midCandidate = list.get(mid);
            if (midCandidate.compareTo(target) > 0) {
                high = mid;
            } else {
                low = mid + 1;
            }
        }
        if (low > 0 && list.get(high - 1).compareTo(target) == 0) {
            return high - 1;
        }
        return -1;
    }


    public static <T> Stream<T> iteratorToStream(Iterator<T> iterator)
    {
       return StreamSupport.stream(
               Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED),
               false
       );
    }

    public static <T> Enumeration<T> streamToEnumeration(Stream<T> stream)
    {
        var it = stream.iterator();
        return new Enumeration<T>() {
            @Override
            public boolean hasMoreElements() {
                return it.hasNext();
            }

            @Override
            public T nextElement() {
                return it.next();
            }
        };
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

    /** Drains all minimal elements from the given queue.
     */
    public static <T> Stream<T> getMinElements(PriorityQueue<T> queue) {
        if (queue.isEmpty()) {
            return Stream.empty();
        }

        T min = queue.peek();
        var comparator = queue.comparator();
        if (comparator == null) {
            // this is an unchecked cast, but any priority queue of elements with
            // no custom comparator, must have a natural order.
            comparator = (Comparator<? super T>)Comparator.naturalOrder();

        }

        Comparator<? super T> finalComparator = comparator;
        return Stream.generate(queue::peek)
                .takeWhile(t -> t != null && finalComparator.compare(min, t) == 0)
                .peek(t -> { queue.poll(); });

    }

    /**
     * Interleaves a collection of streams(using spliterators as they have less overhead)
     * <pre>
     *     Example:
     *     stream1 = [0, 4, 8]
     *     stream2 = [1, 5, 9]
     *     stream3 = [2, 6, 10]
     *     stream4 = [3, 7]
     *
     *     Their interleaving is [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
     *
     * </pre>
     *
     * The number of streams should be finite and relatively small.
     *
     * @param splitsIt Streams to interleave.
     * @param <T> Element type
     * @return Interleaved stream
     */
    public static <T> Spliterator<T> interleave(Iterable<Spliterator<T>> splitsIt) {
        long sizeEstimate = 0;
        /* implementation note: Using an array-list with random deletions.
           This means if there are 's' streams, there's an O(s^2) overhead to
           deleting them. According to https://dzone.com/articles/performance-of-array-vs-linked-list-on-modern-comp
           this is better than using a linked list, in practice.
         */
        var spliterators = new ArrayList<Spliterator<T>>();
        int cs = 0;
        for (var split: splitsIt) {
            sizeEstimate += split.estimateSize();
            cs &= split.estimateSize() & (Spliterator.NONNULL | Spliterator.SIZED);
            spliterators.add(split);
        }
        cs |= Spliterator.ORDERED;


        return new Spliterators.AbstractSpliterator<T>(sizeEstimate, cs) {

            int nextSpliteratorIndex = 0;

            @Override
            public boolean tryAdvance(Consumer<? super T> action) {
                while (!spliterators.isEmpty()) {
                    var spliterator = spliterators.get(nextSpliteratorIndex);
                    if (spliterator.tryAdvance(action)) {
                        nextSpliteratorIndex = (nextSpliteratorIndex + 1) % spliterators.size();
                        return true;
                    } else {
                        spliterators.remove(nextSpliteratorIndex);
                        if (spliterators.isEmpty()) {
                            return false;
                        }
                        nextSpliteratorIndex = nextSpliteratorIndex % spliterators.size();
                    }
                }
                return false;
            }
        };
    }
}
