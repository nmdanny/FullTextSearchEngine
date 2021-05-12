package webdata.dictionary;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Represents the first element in a block, which contains a term pointer
 */
class FirstBlockElement implements DictionaryElement {
    final int frequency;
    final int collectionFrequency;
    final long postingPtr;
    final int suffixLength;
    final int suffixPos;

    static final int SIZE_BYTES = 4 * 4 + 8;

    public FirstBlockElement(int frequency, int collectionFrequency, long postingPtr, int suffixLength, int suffixPos) {
        assert frequency > 0 && postingPtr >= 0 && suffixPos >= 0 && suffixLength >= 0;
        assert collectionFrequency >= frequency;
        this.frequency = frequency;
        this.collectionFrequency = collectionFrequency;
        this.postingPtr = postingPtr;
        this.suffixLength = suffixLength;
        this.suffixPos = suffixPos;
    }

    @Override
    public int getTokenFrequency() {
        return frequency;
    }

    @Override
    public int getTokenCollectionFrequency() {
        return collectionFrequency;
    }

    @Override
    public long getPostingsPointer() {
        return postingPtr;
    }

    public void serialize(DataOutputStream out) throws IOException {
        out.writeInt(frequency);
        out.writeInt(collectionFrequency);
        out.writeLong(postingPtr);
        out.writeInt(suffixLength);
        out.writeInt(suffixPos);
    }

    public static FirstBlockElement deserialize(DataInputStream in) throws IOException {
        int frequency = in.readInt();
        int collectionFrequency = in.readInt();
        long postingPtr = in.readLong();
        int suffixLength = in.readInt();
        int suffixPos = in.readInt();
        return new FirstBlockElement(frequency, collectionFrequency, postingPtr, suffixLength, suffixPos);
    }

    public static FirstBlockElement deserialize(ByteBuffer buf) {
        int frequency = buf.getInt();
        int collectionFrequency = buf.getInt();
        long postingPtr = buf.getLong();
        int suffixLength = buf.getInt();
        int suffixPos = buf.getInt();
        return new FirstBlockElement(frequency, collectionFrequency, postingPtr, suffixLength, suffixPos);
    }

}
