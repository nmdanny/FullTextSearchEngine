package webdata.storage;


import webdata.Utils;
import webdata.sorting.ExternalSorter;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.AbstractList;
import java.util.Collections;
import java.util.Comparator;
import java.util.stream.IntStream;

/** Responsible for mapping between product IDs to pairs of docIDs representing ranges of reviews for said product
 * */
public class ProductIdToDocIdMapper extends AbstractList<ProductIdToDocIdMapper.Pair> implements Closeable, Flushable {

    private static final int PRODUCT_ID_LEN = 10;

    @Override
    public boolean add(Pair pair) {
        throw new UnsupportedOperationException("Use other methods");
    }

    @Override
    public Pair get(int index) {
        return pairStorage.get(index);
    }

    @Override
    public int size() {
        return pairStorage.size();
    }

    public static class Pair {
        int fromDocId;
        int toDocIdIdInclusive;
        byte[] productId;
    }

    private static class PairRecordFactory implements SerializableFactory<Pair> {
        @Override
        public int sizeBytes() {
            return 4 * 2 + PRODUCT_ID_LEN;
        }

        @Override
        public Pair deserialize(ByteBuffer buf) throws IOException {
            var pair = new Pair();
            pair.fromDocId = buf.getInt();
            pair.toDocIdIdInclusive = buf.getInt();
            pair.productId = new byte[PRODUCT_ID_LEN];
            buf.get(pair.productId);
            return pair;
        }

        @Override
        public Pair deserialize(DataInputStream dis) throws IOException {
            var pair = new Pair();
            pair.fromDocId = dis.readInt();
            pair.toDocIdIdInclusive = dis.readInt();
            pair.productId = new byte[PRODUCT_ID_LEN];
            int numRead = dis.read(pair.productId);
            assert numRead == PRODUCT_ID_LEN;
            return pair;
        }

        @Override
        public void serialize(Pair pair, DataOutputStream dos) throws IOException {
            dos.writeInt(pair.fromDocId);
            dos.writeInt(pair.toDocIdIdInclusive);
            dos.write(pair.productId);
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

    private final Path dir;
    private final PairStorage pairStorage;
    private final CharsetEncoder productIdEncoder;
    private final CharsetDecoder productIdDecoder;

    String curProductId;
    int curProductStartingDocId;
    int curProductMaxDocId;

    public ProductIdToDocIdMapper(String dir) throws IOException {
        this.dir = Path.of(dir);
        this.pairStorage = new PairStorage(dir);
        // Even if our document is UTF-8 or something else, the product ID only consists of
        // latin ascii characters
        this.productIdEncoder = StandardCharsets.US_ASCII.newEncoder();
        this.productIdDecoder = StandardCharsets.US_ASCII.newDecoder();

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
            try {
                return productIdDecoder.decode(ByteBuffer.wrap(pair.productId)).toString();
            } catch (CharacterCodingException e) {
                throw new RuntimeException("Impossible: decoded productID is not ASCII", e);
            }
        }

        @Override
        public int size() {
            return pairStorage.size();
        }
    };

    /** If given productID wasn't seen, adds it to the storage, otherwise,
     *  updates the 'toDocIdInclusive' field of the entry corresponding to given element.
     */
    public void observeProduct(String productId, int docId) {
        if (curProductId == null) {
            beginPairForProduct(productId, docId);
        } else if (curProductId.equals(productId)) {
            assert docId > curProductMaxDocId;
            curProductMaxDocId = docId;
        } else {
            beginPairForProduct(productId, docId);
        }
    }

    /**
     * Performs external sort on all pairs by productID. Note, this invalidates the stream returned
     * by 'getReviewIdsForProduct'
     * @throws IOException In case of IO error during sorting
     */
    public void externalSort() throws IOException {
        endPairForCurrentProduct();
        flush();
        pairStorage.externalSort(Comparator.comparing(record -> {
            try {
              return productIdDecoder.decode(ByteBuffer.wrap(record.productId)).toString();
            } catch (CharacterCodingException ex) {
                throw new RuntimeException("Impossible, couldn't decode productID as ASCII", ex);
            }
        }));
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
        try {
            var productIdBuf = productIdEncoder.encode(CharBuffer.wrap(curProductId));
            assert productIdBuf.limit() == PRODUCT_ID_LEN;
            pair.productId = productIdBuf.array();
        } catch (CharacterCodingException e) {
            throw new RuntimeException("Impossible - got invalid characters in productId", e);
        }
        this.pairStorage.add(pair);

        this.curProductId = null;
        this.curProductStartingDocId = 0;
        this.curProductMaxDocId = 0;
    }

    /** Returns a stream of document IDs for given product,
     *  assuming the storage is sorted. */
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
