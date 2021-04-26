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
    final int postingPtr;
    final int suffixLength;
    final int suffixPos;

    static final int SIZE_BYTES = 4 * 4;

    public FirstBlockElement(int frequency, int postingPtr, int suffixLength, int suffixPos) {
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
    public int getPostingsPointer() {
        return postingPtr;
    }

    public void serialize(DataOutputStream out) throws IOException {
        out.writeInt(frequency);
        out.writeInt(postingPtr);
        out.writeInt(suffixLength);
        out.writeInt(suffixPos);
    }

    public static FirstBlockElement deserialize(DataInputStream in) throws IOException {
        int frequency = in.readInt();
        int postingPtr = in.readInt();
        int suffixLength = in.readInt();
        int suffixPos = in.readInt();
        return new FirstBlockElement(frequency, postingPtr, suffixLength, suffixPos);
    }

    public static FirstBlockElement deserialize(ByteBuffer buf) {
        int frequency = buf.getInt();
        int postingPtr = buf.getInt();
        int suffixLength = buf.getInt();
        int suffixPos = buf.getInt();
        return new FirstBlockElement(frequency, postingPtr, suffixLength, suffixPos);
    }

}
