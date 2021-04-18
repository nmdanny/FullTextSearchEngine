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

/** Allows retrieving review metadata(anything other than text) by
 *  docID in O(1), as well as sequentially building the storage from
 *  increasing docIDs.
 */
public class ReviewStorage extends FixedRecordStorage<CompactReview> {
    private static final String STORAGE_FILE = "storage.bin";

    public ReviewStorage(Path path) throws IOException {
       super(path.toString(), new SerializableFactory<CompactReview>() {
           @Override
           public int sizeBytes() {
               return CompactReview.SIZE_BYTES;
           }

           @Override
           public CompactReview deserialize(DataInputStream dis) throws IOException {
               throw new UnsupportedEncodingException();
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
