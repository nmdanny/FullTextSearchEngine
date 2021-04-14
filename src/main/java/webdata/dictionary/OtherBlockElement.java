package webdata.dictionary;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Represents a block element which isn't the first
 */
class OtherBlockElement implements DictionaryElement {
    final int frequency;
    final int postingPtr;
    final int prefixLength;
    final int suffixLength;

    static final int SIZE_BYTES = 4 * 4;

    public OtherBlockElement(int frequency, int postingPtr, int prefixLength, int suffixLength) {
        this.frequency = frequency;
        this.postingPtr = postingPtr;
        this.prefixLength = prefixLength;
        this.suffixLength = suffixLength;
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
        out.writeInt(prefixLength);
        out.writeInt(suffixLength);
    }

    public static OtherBlockElement deserialize(DataInputStream in) throws IOException {
        int frequency = in.readInt();
        int postingPtr = in.readInt();
        int prefixLength = in.readInt();
        int suffixLength = in.readInt();
        return new OtherBlockElement(frequency, postingPtr, prefixLength, suffixLength);
    }
}
