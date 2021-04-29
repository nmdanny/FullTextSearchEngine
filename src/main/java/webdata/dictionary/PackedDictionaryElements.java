package webdata.dictionary;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.AbstractList;
import java.util.RandomAccess;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;


/** A packed list of dictionary elements in memory, allowing readonly list operations */
class PackedDictionaryElements extends AbstractList<DictionaryElement> implements RandomAccess {

    private final byte[] packedBytes;
    // wraps packedBytes
    private final ByteBuffer byteBuffer;
    private final int numFirstBlockElement;
    private final int numOtherBlockElement;

    private static final int NUM_BYTES_PER_BLOCK = FirstBlockElement.SIZE_BYTES + ((Dictionary.BLOCK_SIZE - 1) * OtherBlockElement.SIZE_BYTES);

    /** Reads the given number of dictionary elements from the input stream, loading
     *  their packed representation into memory
     */
    PackedDictionaryElements(DataInputStream dis, int numElements) throws IOException {
        int numFirstBlockElement = 0;
        int numOtherBlockElement = 0;

        FirstBlockElement lastFbe = null;
        try (   var byteArrayOs = new ByteArrayOutputStream();
                var dos = new DataOutputStream(byteArrayOs)) {
            for (int elementNum = 0; elementNum < numElements; ++elementNum) {
                if (elementNum % Dictionary.BLOCK_SIZE == 0) {
                    lastFbe = FirstBlockElement.deserialize(dis);
                    lastFbe.serialize(dos);
                    ++numFirstBlockElement;
                } else {
                    OtherBlockElement.deserialize(lastFbe, dis).serialize(dos);
                    ++numOtherBlockElement;
                }
            }
            this.packedBytes = byteArrayOs.toByteArray();
            this.byteBuffer = ByteBuffer.wrap(packedBytes);
            this.numFirstBlockElement = numFirstBlockElement;
            this.numOtherBlockElement = numOtherBlockElement;
            assert this.packedBytes.length == this.numFirstBlockElement * FirstBlockElement.SIZE_BYTES + this.numOtherBlockElement * OtherBlockElement.SIZE_BYTES;
        }
    }

    private int getByteIndexOfBlockBeginning(int blockIndex) {
        if (blockIndex == 0) {
            return 0;
        }
        return blockIndex * NUM_BYTES_PER_BLOCK;
    }

    private int getByteOffsetOfElementInBlock(int posInBlock) {
        int offset = 0;
        if (posInBlock > 0) {
            offset += FirstBlockElement.SIZE_BYTES;
        }
        if (posInBlock > 1) {
            int numOtherElements = posInBlock - 1;
            offset += numOtherElements * OtherBlockElement.SIZE_BYTES;
        }
        return offset;
    }


    @Override
    public DictionaryElement get(int index) {
        assert index >= 0 && index < size();

        int blockIx = index / Dictionary.BLOCK_SIZE;
        int posInBlock = index % Dictionary.BLOCK_SIZE;
        int bytePosOfBeginning = getByteIndexOfBlockBeginning(blockIx);
        int byteOffsetOfElementInBlock = getByteOffsetOfElementInBlock(posInBlock);


        byteBuffer.position(bytePosOfBeginning);
        var fbe = FirstBlockElement.deserialize(byteBuffer);
        if (byteOffsetOfElementInBlock == 0) {
            return fbe;
        } else {
            byteBuffer.position(bytePosOfBeginning + byteOffsetOfElementInBlock);
            return OtherBlockElement.deserialize(fbe, byteBuffer);
        }
    }

    @Override
    public int size() {
        return numFirstBlockElement + numOtherBlockElement;
    }

    /** A spliterator over all elements in this dictionary */
    @Override
    public Spliterator<DictionaryElement> spliterator() {
        int numElements = size();
        int characteristics = Spliterator.ORDERED | Spliterator.NONNULL | Spliterator.SIZED;
        return new Spliterators.AbstractSpliterator<DictionaryElement>(numElements, characteristics) {
            int elemIx = 0;
            final ByteArrayInputStream bais = new ByteArrayInputStream(packedBytes);
            final DataInputStream dis = new DataInputStream(bais);
            FirstBlockElement lastFbe = null;

            @Override
            public boolean tryAdvance(Consumer<? super DictionaryElement> action) {
                if (elemIx >= numElements) {
                    return false;
                }
                try {
                    DictionaryElement element;
                    if (elemIx % Dictionary.BLOCK_SIZE == 0) {
                        lastFbe = FirstBlockElement.deserialize(dis);
                        element = lastFbe;
                    } else {
                        element = OtherBlockElement.deserialize(lastFbe, dis);
                    }
                    action.accept(element);
                    ++elemIx;
                    return true;
                } catch (IOException ex) {
                    throw new RuntimeException("Impossible, IO exception dealing with in memory byte array", ex);
                }
            }
        };
    }
}
