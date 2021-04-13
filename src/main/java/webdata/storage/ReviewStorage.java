package webdata.storage;

import webdata.Utils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ReviewStorage extends FixedRecordStorage<CompactReview> {
    private static final String STORAGE_FILE = "storage.bin";

    public ReviewStorage(Path path) throws IOException {
       super(path.toString(), new FixedSizeRecordFactory<CompactReview>() {
           @Override
           public int sizeBytes() {
               return CompactReview.SIZE_BYTES;
           }

           @Override
           public CompactReview deserialize(ByteBuffer buf) throws IOException {
               return CompactReview.deserialize(buf);
           }

           @Override
           public void serialize(CompactReview compactReview, DataOutputStream dos) throws IOException {
               compactReview.serialize(dos);
           }
       }, null);
   }


    public void appendMany(Stream<CompactReview> reviews) throws IOException {
        reviews.forEachOrdered(this::add);
    }

   public static ReviewStorage inDirectory(String dir) throws IOException {
       return new ReviewStorage(Path.of(dir, STORAGE_FILE));
   }

   public int getNumReviews() {
        return size();
   }
}

/** Stores reviews sorted by docID (original input file insertion order), allowing O(1) access to fields of a
 *  review with a given docID. */
class OldReviewStorage implements Closeable, Flushable {
    private static final String STORAGE_FILE = "storage.bin";

    private final String path;

    // Review buffer used for reading
    private final ByteBuffer reviewBuf;

    private final RandomAccessFile file;
    private final FileChannel channel;
    private int numReviews;

    /** Allows reading and writing compact reviews(without terms) to a binary file */
    public OldReviewStorage(Path path) throws IOException {
        this.path = path.toString();
        this.file = new RandomAccessFile(this.path, "rw");
        this.channel = file.getChannel();
        this.reviewBuf = ByteBuffer.allocate(CompactReview.SIZE_BYTES);
        this.numReviews = (int)(file.length() / (long)CompactReview.SIZE_BYTES);
    }

    public static ReviewStorage inDirectory(String dir) throws IOException {
        return new ReviewStorage(Path.of(dir, STORAGE_FILE));
    }

    /** Returns the number of reviews in the storage */
    public int getNumReviews() {
        return numReviews;
    }

    /** Appends a stream of reviews(assumed to be ordered via their docIDs) into the storage */
    public void appendMany(Stream<CompactReview> reviews) throws IOException {
        channel.position(file.length());

        try (   var bulkAppendFile = new RandomAccessFile(this.path, "rw");
                var bulkAppendChannel = bulkAppendFile.getChannel();
                var stream = new DataOutputStream(new BufferedOutputStream(Channels.newOutputStream(bulkAppendChannel))) ) {
            var it = reviews.iterator();
            CompactReview lastReview = null;
            while (it.hasNext()) {
                var compactReview = it.next();

//                if (lastReview != null && compactReview.getProductId().compareTo(lastReview.getProductId()) < 0) {
//                    throw new IllegalArgumentException("Must append reviews in a lexicographically increasing product ID order");
//                }
                lastReview = compactReview;

                if (numReviews % 1000000 == 0) {
//                    System.out.format("+1mil Finished %d reviews so far\n", numReviews);
                }
                if (numReviews % 5000000 == 0) {
//                    System.out.format("+5mil Finished %d reviews so far, flushing\n", numReviews);
                    stream.flush();
                    channel.force(true);
                }
                compactReview.serialize(stream);
                ++numReviews;
            }
        }
    }



    /** Loads a compact review from storage file using given document ID */
    public CompactReview get(int docId) throws IOException {
        if (docId <= 0) {
            throw new IllegalArgumentException("docId must be positive");
        }
        long filePos = docIdToFilePos(docId);
        channel.position(filePos);
        reviewBuf.clear();
        int bytesRead = channel.read(reviewBuf);
        reviewBuf.flip();
        assert bytesRead == CompactReview.SIZE_BYTES;
        return CompactReview.deserialize(reviewBuf);
    }


    @Override
    public void close() throws IOException {
        channel.close();;
        file.close();
    }

    @Override
    public void flush() throws IOException {
        channel.force(true);
    }

    private long docIdToFilePos(int docId) {
        // docIds begin from 1, hence why we subtract one
        return ((long)docId - 1) * CompactReview.SIZE_BYTES;
    }

    // Returns a tuple of the lowest and highest document IDs matching given product Id, or an empty array otherwise.
    public int[] binarySearchRange(String productId) throws IOException {
        var abstractList = new AbstractList<String>() {
            @Override
            public String get(int index) {
                try {
                    return OldReviewStorage.this.get(index + 1).getProductId();
                } catch (IOException ex) {
                    throw new RuntimeException(String.format("Got IO error while getting document of ID %d", index + 1));
                }
            }

            @Override
            public int size() {
                return numReviews;
            }
        };

        /* Now, perform binary search on documents [1, firstDocId-1]
           The reason for doing a binary search, rather than linearly scanning backwards,
           is that despite both having the same complexity, if this product has lots of reviews and we happened to
           land far from the beginning, reading those reviews via random access is slow.

           I would like to utilize buffering, but Java's built in buffered streams only go in the forward direction. Hence
           why I'd like to quickly find the lowest matching docId by skipping around
         */

        int lowestMatchingDocId = 1 + Utils.binarySearchLeftmost(abstractList, productId);
        if (lowestMatchingDocId <= 0) {
            return new int[]{};
        }
        int highestMatchingDocId = 1 + Utils.binarySearchRightmost(abstractList, productId);

        assert highestMatchingDocId >= lowestMatchingDocId;
        assert get(lowestMatchingDocId).getProductId().equals(productId);
        assert get(highestMatchingDocId).getProductId().equals(productId);
        if (lowestMatchingDocId > 1) {
            assert !get(lowestMatchingDocId - 1).getProductId().equals(productId);
        }
        if (highestMatchingDocId + 1 <= numReviews) {
            assert !get(highestMatchingDocId + 1).getProductId().equals(productId);
        }

        return new int[] { lowestMatchingDocId, highestMatchingDocId };
    }

    /** Returns a stream of document IDs for given product */
    public IntStream getReviewsForProduct(String productId) throws IOException {
        int[] low_high = binarySearchRange(productId);
        if (low_high.length == 0) {
            return IntStream.empty();
        }
        return IntStream.rangeClosed(low_high[0], low_high[1]);
    }

}
