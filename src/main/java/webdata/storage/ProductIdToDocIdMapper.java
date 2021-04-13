package webdata.storage;


import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.AbstractList;
import java.util.Collections;
import java.util.stream.IntStream;

/** Responsible for mapping between product IDs to pairs of docIDs representing ranges of reviews for said product
 * */
public class ProductIdToDocIdMapper implements Closeable, Flushable {

    private static class Pair {
        int fromDocId;
        int toDocIdIdInclusive;
    }

    private static class PairRecordFactory implements FixedSizeRecordFactory<Pair> {
        @Override
        public int sizeBytes() {
            return 4 * 2;
        }

        @Override
        public Pair deserialize(ByteBuffer buf) throws IOException {
            var pair = new Pair();
            pair.fromDocId = buf.getInt();
            pair.toDocIdIdInclusive = buf.getInt();
            return pair;
        }

        @Override
        public void serialize(Pair pair, DataOutputStream dos) throws IOException {
            dos.writeInt(pair.fromDocId);
            dos.writeInt(pair.toDocIdIdInclusive);
        }
    }

    private static final String PAIR_STORAGE_FILE = "docID_pairs_sorted_by_productIDs.bin";

    private static class PairStorage extends FixedRecordStorage<Pair> {
        PairStorage(String dir) throws IOException {
            super(Path.of(dir, PAIR_STORAGE_FILE).toString(),
                    new PairRecordFactory(),
                    null);//Comparator.comparing(pair -> abstractList.get(pair.fromDocId)));
        }
    }

    private final PairStorage pairStorage;
    private final ReviewStorage reviewStorage;

    String curProductId;
    int curProductStartingDocId;
    int curProductMaxDocId;

    public ProductIdToDocIdMapper(String dir, ReviewStorage reviewStorage) throws IOException {
        this.pairStorage = new PairStorage(dir);
        this.reviewStorage = reviewStorage;

        this.curProductId = null;
        this.curProductStartingDocId = 0;
    }

    // Allows treating our pair storage as an ordered list of product IDs, by using
    // the fact that the pairs in the PairStorage are ordered by productIDs, while
    // the productIDs themselves are accessed via IO from the ReviewStorage.
    //
    // This in turn allows us to use binary search with a string(a product ID) via Java's standard collection
    // methods.
    private final AbstractList<String> abstractList = new AbstractList<String>() {
        @Override
        public String get(int index) {
            Pair pair = pairStorage.get(index);
            return reviewStorage.get(pair.fromDocId - 1).getProductId();
        }

        @Override
        public int size() {
            return pairStorage.size();
        }
    };

    public void observeProduct(String productId, int docId) {
        if (curProductId == null) {
            beginPairForProduct(productId, docId);
        } else if (curProductId.equals(productId)) {
            assert docId > curProductMaxDocId;
            curProductMaxDocId = docId;
        } else {
            assert productId.compareTo(curProductId) >= 0;
            beginPairForProduct(productId, docId);
        }
    }

    private void beginPairForProduct(String productId, int firstDocId)  {
        assert firstDocId >= 1;
        endPairForCurrentProduct();

        this.curProductId = productId;
        this.curProductStartingDocId = firstDocId;
        this.curProductMaxDocId = firstDocId;
    }

    private void endPairForCurrentProduct() {
        if (curProductId == null) {
            return;
        }
        assert curProductMaxDocId >= curProductStartingDocId;


        var pair = new Pair();
        pair.fromDocId = curProductStartingDocId;
        pair.toDocIdIdInclusive = curProductMaxDocId;
        this.pairStorage.add(pair);

        this.curProductId = null;
        this.curProductStartingDocId = 0;
        this.curProductMaxDocId = 0;
    }

    /** Returns a stream of document IDs for given product */
    public IntStream getReviewIdsForProduct(String productID) {
        int pairStorageIndex = Collections.binarySearch(
                abstractList,
                productID.toLowerCase()
        );

        if (pairStorageIndex < 0) {
            return IntStream.empty();
        }
        var pair = pairStorage.get(pairStorageIndex);
        return IntStream.rangeClosed(pair.fromDocId, pair.toDocIdIdInclusive);
    }

    @Override
    public void close() throws IOException {
        endPairForCurrentProduct();
        flush();
        pairStorage.close();
    }

    @Override
    public void flush() throws IOException {
        pairStorage.flush();
    }
}
