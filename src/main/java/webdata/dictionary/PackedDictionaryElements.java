package webdata.dictionary;

import java.io.*;
import java.util.AbstractList;


/** A packed list of dictionary elements in memory, allowing readonly list operations */
class PackedDictionaryElements extends AbstractList<DictionaryElement> {

    private final byte[] packedBytes;
    private final int numFirstBlockElement;
    private final int numOtherBlockElement;

    private static final int NUM_BYTES_PER_BLOCK = FirstBlockElement.SIZE_BYTES + ((Dictionary.BLOCK_SIZE - 1) * OtherBlockElement.SIZE_BYTES);

    /** Reads the given number of dictionary elements from the input stream, loading
     *  their packed representation into memory
     */
    PackedDictionaryElements(DataInputStream dis, int numElements) throws IOException {
        int numFirstBlockElement = 0;
        int numOtherBlockElement = 0;

        try (   var byteArrayOs = new ByteArrayOutputStream();
                var dos = new DataOutputStream(byteArrayOs)) {
            for (int elementNum = 0; elementNum < numElements; ++elementNum) {
                if (elementNum % Dictionary.BLOCK_SIZE == 0) {
                    FirstBlockElement.deserialize(dis).serialize(dos);
                    ++numFirstBlockElement;
                } else {
                    OtherBlockElement.deserialize(dis).serialize(dos);
                    ++numOtherBlockElement;
                }
            }
            this.packedBytes = byteArrayOs.toByteArray();
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

        int blockIx = index / Dictionary.BLOCK_SIZE;
        int posInBlock = index % Dictionary.BLOCK_SIZE;
        int bytePos = getByteIndexOfBlockBeginning(blockIx) + getByteOffsetOfElementInBlock(posInBlock);


        try (var is = new ByteArrayInputStream(packedBytes);
             var dis = new DataInputStream(is)) {
            long skipped = is.skip(bytePos);
            assert skipped == bytePos;

            if (index % Dictionary.BLOCK_SIZE == 0) {
                return FirstBlockElement.deserialize(dis);
            } else {
                return OtherBlockElement.deserialize(dis);
            }
        } catch (IOException ex) {
            throw new RuntimeException("Impossible, got IOException while operating on ByteArrayInputStream");
        }
    }

    @Override
    public int size() {
        return numFirstBlockElement + numOtherBlockElement;
    }
}
