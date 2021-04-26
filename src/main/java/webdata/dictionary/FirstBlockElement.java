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
    final long postingPtr;
    final int suffixLength;
    final int suffixPos;

    static final int SIZE_BYTES = 4 * 3 + 8;

    public FirstBlockElement(int frequency, long postingPtr, int suffixLength, int suffixPos) {
        assert frequency > 0 && postingPtr >= 0 && suffixPos >= 0 && suffixLength >= 0;
        this.frequency = frequency;
        this.postingPtr = postingPtr;
        this.suffixLength = suffixLength;
        this.suffixPos = suffixPos;
    }

    @Override
    public int getTokenFrequency() {
        return frequency;
    }

    @Override
    public long getPostingsPointer() {
        return postingPtr;
    }

    public void serialize(DataOutputStream out) throws IOException {
        out.writeInt(frequency);
        out.writeLong(postingPtr);
        out.writeInt(suffixLength);
        out.writeInt(suffixPos);
    }

    public static FirstBlockElement deserialize(DataInputStream in) throws IOException {
        int frequency = in.readInt();
        long postingPtr = in.readLong();
        int suffixLength = in.readInt();
        int suffixPos = in.readInt();
        return new FirstBlockElement(frequency, postingPtr, suffixLength, suffixPos);
    }

    public static FirstBlockElement deserialize(ByteBuffer buf) {
        int frequency = buf.getInt();
        long postingPtr = buf.getLong();
        int suffixLength = buf.getInt();
        int suffixPos = buf.getInt();
        return new FirstBlockElement(frequency, postingPtr, suffixLength, suffixPos);
    }

}
